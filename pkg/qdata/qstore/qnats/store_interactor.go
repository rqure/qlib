package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qvalue"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type NatsStoreInteractor struct {
	core NatsCore
}

func NewStoreInteractor(core NatsCore) qdata.StoreInteractor {
	return &NatsStoreInteractor{core: core}
}

func (me *NatsStoreInteractor) CreateEntity(ctx context.Context, entityType, parentId, name string) string {
	msg := &qprotobufs.ApiConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
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

func (me *NatsStoreInteractor) GetEntity(ctx context.Context, entityId string) *qdata.Entity {
	msg := &qprotobufs.ApiConfigGetEntityRequest{
		Id: entityId,
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

func (me *NatsStoreInteractor) DeleteEntity(ctx context.Context, entityId string) {
	msg := &qprotobufs.ApiConfigDeleteEntityRequest{
		Id: entityId,
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to delete entity: %v", err)
	}
}

func (me *NatsStoreInteractor) FindEntities(ctx context.Context, entityType string) []string {
	msg := &qprotobufs.ApiRuntimeGetEntitiesRequest{
		EntityType: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiRuntimeGetEntitiesResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	ids := make([]string, len(response.Entities))
	for i, e := range response.Entities {
		ids[i] = e.Id
	}
	return ids
}

func (me *NatsStoreInteractor) GetEntityTypes(ctx context.Context) []string {
	msg := &qprotobufs.ApiConfigGetEntityTypesRequest{}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigGetEntityTypesResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	return response.Types
}

func (me *NatsStoreInteractor) EntityExists(ctx context.Context, entityId string) bool {
	msg := &qprotobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId,
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
		requests[i].Value.Update(qvalue.FromAnyPb(r.Value))
		requests[i].Success = r.Success
		if r.WriteTime != nil && r.WriteTime.Raw != nil {
			requests[i].WriteTime.Update(r.WriteTime.Raw.AsTime())
		}
		if r.WriterId != nil {
			requests[i].WriterId.Update(r.WriterId.Raw)
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

func (me *NatsStoreInteractor) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	msg := &qprotobufs.ApiRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
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

func (me *NatsStoreInteractor) GetEntitySchema(ctx context.Context, entityType string) *qdata.EntitySchema {
	msg := &qprotobufs.ApiConfigGetEntitySchemaRequest{
		Type: entityType,
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
		Schema: qentity.ToSchemaPb(schema),
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to set entity schema: %v", err)
	}
}

func (me *NatsStoreInteractor) GetFieldSchema(ctx context.Context, entityType, fieldName string) *qdata.FieldSchema {
	schema := me.GetEntitySchema(ctx, entityType)
	if schema == nil {
		return nil
	}

	return schema.GetField(fieldName)
}

func (me *NatsStoreInteractor) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema *qdata.FieldSchema) {
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		qlog.Error("Failed to get entity schema for type %s", entityType)
		return
	}

	fields := entitySchema.GetFields()
	updated := false
	for i, f := range fields {
		if f.GetFieldName() == fieldName {
			fields[i] = schema
			updated = true
			break
		}
	}

	if !updated {
		fields = append(fields, schema)
	}

	entitySchema.SetFields(fields)
	me.SetEntitySchema(ctx, entitySchema)
}

func (me *NatsStoreInteractor) PublishNotifications(ctx context.Context, curr *qdata.Request, prev *qdata.Request) {

}
