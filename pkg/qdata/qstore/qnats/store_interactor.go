package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type NatsStoreInteractor struct {
	core NatsCore
}

func NewStoreInteractor(core NatsCore) qdata.StoreInteractor {
	return &NatsStoreInteractor{core: core}
}

func (me *NatsStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) string {
	msg := &qprotobufs.ApiConfigCreateEntityRequest{
		Type:     string(entityType),
		ParentId: string(parentId),
		Name:     name,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to create entity: %v", err)
	}

	var response qprotobufs.ApiConfigCreateEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		qlog.Error("Failed to create entity: %v", err)
	}

	if response.Status != qprotobufs.ApiConfigCreateEntityResponse_SUCCESS {
		qlog.Error("Failed to create entity: %v", response.Status)
	}

	return response.Id
}

func (me *NatsStoreInteractor) GetEntity(ctx context.Context, entityId qdata.EntityId) *qdata.Entity {
	msg := &qprotobufs.ApiConfigGetEntityRequest{
		Id: string(entityId),
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigGetEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != qprotobufs.ApiConfigGetEntityResponse_SUCCESS {
		return nil
	}

	return new(qdata.Entity).FromEntityPb(response.Entity)
}

func (me *NatsStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) {
	msg := &qprotobufs.ApiConfigDeleteEntityRequest{
		Id: string(entityId),
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to delete entity: %v", err)
	}
}

func (me *NatsStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) *qdata.PageResult[qdata.EntityId] {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	return &qdata.PageResult[qdata.EntityId]{
		Items:   []qdata.EntityId{},
		HasMore: true,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
			msg := &qprotobufs.ApiRuntimeFindEntitiesRequest{
				EntityType: entityType.AsString(),
				PageSize:   pageConfig.PageSize,
				Cursor:     pageConfig.CursorId,
			}

			resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
			if err != nil {
				return nil, err
			}

			var response qprotobufs.ApiRuntimeFindEntitiesResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, err
			}

			entities := make([]qdata.EntityId, 0, len(response.Entities))
			for _, id := range response.Entities {
				entities = append(entities, qdata.EntityId(id))
			}

			pageConfig.CursorId = response.NextCursor

			return &qdata.PageResult[qdata.EntityId]{
				Items:   entities,
				HasMore: response.HasMore,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
					return me.FindEntities(entityType, pageOpts...), nil
				},
			}, nil
		},
	}
}

func (me *NatsStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) *qdata.PageResult[qdata.EntityType] {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	return &qdata.PageResult[qdata.EntityType]{
		Items:   []qdata.EntityType{},
		HasMore: true,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
			msg := &qprotobufs.ApiRuntimeGetEntityTypesRequest{
				PageSize: pageConfig.PageSize,
				Cursor:   pageConfig.CursorId,
			}

			resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
			if err != nil {
				return nil, err
			}

			var response qprotobufs.ApiRuntimeGetEntityTypesResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, err
			}

			types := make([]qdata.EntityType, 0, len(response.EntityTypes))
			for _, t := range response.EntityTypes {
				types = append(types, qdata.EntityType(t))
			}

			pageConfig.CursorId = response.NextCursor

			return &qdata.PageResult[qdata.EntityType]{
				Items:   types,
				HasMore: response.HasMore,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
					return me.GetEntityTypes(pageConfig.IntoOpts()...), nil
				},
			}, nil
		},
	}
}

func (me *NatsStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) bool {
	msg := &qprotobufs.ApiRuntimeEntityExistsRequest{
		EntityId: string(entityId),
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response qprotobufs.ApiRuntimeEntityExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

func (me *NatsStoreInteractor) Read(ctx context.Context, requests ...*qdata.Request) {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_READ,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = r.AsRequestPb()
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return
	}

	var response qprotobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return
	}

	for i, r := range response.Response {
		if i >= len(requests) {
			break
		}
		requests[i].Value.FromAnyPb(r.Value)
		requests[i].Success = r.Success
		if r.WriteTime != nil && r.WriteTime.Raw != nil {
			requests[i].WriteTime.FromTime(r.WriteTime.Raw.AsTime())
		}
		if r.WriterId != nil {
			requests[i].WriterId.FromString(r.WriterId.Raw)
		}
	}
}

func (me *NatsStoreInteractor) Write(ctx context.Context, requests ...*qdata.Request) {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_WRITE,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		writer := r.WriterId
		if writer == nil || *writer == "" {
			if me.clientId == nil {
				clients := qquery.New(&qdata.LimitedStore{
					NatsStoreInteractor:   me,
					EntityManager:         me.entityManager,
					NotificationPublisher: me.notificationPublisher,
					NatsStoreInteractor:   me.NatsStoreInteractor,
				}).Select().
					From("Client").
					Where("Name").Equals(qapp.GetName()).
					Execute(ctx)

				if len(clients) == 0 {
					qlog.Error("Failed to get client id")
				} else {
					if len(clients) > 1 {
						qlog.Warn("Multiple clients found: %v", clients)
					}

					clientId := clients[0].GetId()
					me.clientId = &clientId
				}
			}

			if me.clientId != nil {
				r.SetWriter(me.clientId)
			}
		}
		msg.Requests[i] = r.AsRequestPb()
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		return
	}

	var response qprotobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return
	}

	for i, r := range response.Response {
		if i >= len(requests) {
			break
		}
		requests[i].Success = r.Success
	}
}

func (me *NatsStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) bool {
	msg := &qprotobufs.ApiRuntimeFieldExistsRequest{
		FieldName:  string(fieldType),
		EntityType: string(entityType),
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response qprotobufs.ApiRuntimeFieldExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

func (me *NatsStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) *qdata.EntitySchema {
	msg := &qprotobufs.ApiConfigGetEntitySchemaRequest{
		Type: string(entityType),
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigGetEntitySchemaResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != qprotobufs.ApiConfigGetEntitySchemaResponse_SUCCESS {
		return nil
	}

	return qentity.FromSchemaPb(response.Schema)
}

func (me *NatsStoreInteractor) SetEntitySchema(ctx context.Context, schema *qdata.EntitySchema) {
	msg := &qprotobufs.ApiConfigSetEntitySchemaRequest{
		Schema: schema.AsEntitySchemaPb(),
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to set entity schema: %v", err)
	}
}

func (me *NatsStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) *qdata.FieldSchema {
	schema := me.GetEntitySchema(ctx, entityType)
	if schema == nil {
		return nil
	}

	return schema.Fields[fieldType]
}

func (me *NatsStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) {
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		qlog.Error("Failed to get entity schema for type %s", entityType)
		return
	}

	entitySchema.Fields[fieldType] = schema

	me.SetEntitySchema(ctx, entitySchema)
}

func (me *NatsStoreInteractor) PublishNotifications(ctx context.Context, curr *qdata.Request, prev *qdata.Request) {

}
