package qnats

import (
	"context"
	"fmt"

	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
)

type NatsStoreInteractor struct {
	core       NatsCore
	publishSig qss.Signal[qdata.PublishNotificationArgs]
	clientId   *qdata.EntityId
}

func NewStoreInteractor(core NatsCore) qdata.StoreInteractor {
	return &NatsStoreInteractor{
		core:       core,
		publishSig: qss.New[qdata.PublishNotificationArgs](),
	}
}

func (me *NatsStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) qdata.EntityId {
	msg := &qprotobufs.ApiConfigCreateEntityRequest{
		Type:     entityType.AsString(),
		ParentId: parentId.AsString(),
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

	return *new(qdata.EntityId).FromString(response.Id)
}

func (me *NatsStoreInteractor) GetEntity(ctx context.Context, entityId qdata.EntityId) *qdata.Entity {
	msg := &qprotobufs.ApiConfigGetEntityRequest{
		Id: entityId.AsString(),
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
		Id: entityId.AsString(),
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to delete entity: %v", err)
	}
}

func (me *NatsStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) *qdata.PageResult[qdata.EntityId] {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityId]{
		Items:    []qdata.EntityId{},
		CursorId: pageConfig.CursorId,
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

			nextCursor := response.NextCursor

			return &qdata.PageResult[qdata.EntityId]{
				Items:    entities,
				CursorId: nextCursor,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
					if nextCursor < 0 {
						return &qdata.PageResult[qdata.EntityId]{
							Items:    []qdata.EntityId{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					return me.FindEntities(entityType,
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursor)).NextPage(ctx)
				},
			}, nil
		},
	}
}

func (me *NatsStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) *qdata.PageResult[qdata.EntityType] {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityType]{
		Items:    []qdata.EntityType{},
		CursorId: pageConfig.CursorId,
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

			nextCursor := response.NextCursor

			return &qdata.PageResult[qdata.EntityType]{
				Items:    types,
				CursorId: nextCursor,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
					return me.GetEntityTypes(
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursor)).NextPage(ctx)
				},
			}, nil
		},
	}
}

func (me *NatsStoreInteractor) PrepareQuery(sql string, args ...interface{}) *qdata.PageResult[*qdata.Entity] {
	pageOpts := []qdata.PageOpts{}
	typeHintOpts := []qdata.TypeHintOpts{}
	otherArgs := []interface{}{}

	for _, arg := range args {
		switch arg := arg.(type) {
		case qdata.PageOpts:
			pageOpts = append(pageOpts, arg)
		case qdata.TypeHintOpts:
			typeHintOpts = append(typeHintOpts, arg)
		default:
			otherArgs = append(otherArgs, arg)
		}
	}

	// Format the query with args
	formattedQuery := fmt.Sprintf(sql, otherArgs...)
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	// Convert type hints to protobuf format
	typeHints := []*qprotobufs.TypeHint{}
	typeHintMap := make(qdata.TypeHintMap)
	for _, opt := range typeHintOpts {
		opt(typeHintMap)
	}

	for fieldType, valueType := range typeHintMap {
		typeHints = append(typeHints, &qprotobufs.TypeHint{
			FieldType: fieldType.AsString(),
			ValueType: valueType.AsString(),
		})
	}

	return &qdata.PageResult[*qdata.Entity]{
		Items:    []*qdata.Entity{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[*qdata.Entity], error) {
			// Create a query request message
			msg := &qprotobufs.ApiRuntimeQueryRequest{
				Query:     formattedQuery,
				PageSize:  pageConfig.PageSize,
				Cursor:    pageConfig.CursorId,
				TypeHints: typeHints,
			}

			// Send the query to the server
			resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
			if err != nil {
				return nil, fmt.Errorf("failed to execute query: %v", err)
			}

			// Parse the response
			var response qprotobufs.ApiRuntimeQueryResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, fmt.Errorf("failed to parse query response: %v", err)
			}

			// Convert entities from protobuf
			entities := make([]*qdata.Entity, 0, len(response.Entities))
			for _, entityPb := range response.Entities {
				entity := new(qdata.Entity).FromEntityPb(entityPb)
				entities = append(entities, entity)
			}

			nextCursor := response.NextCursor

			return &qdata.PageResult[*qdata.Entity]{
				Items:    entities,
				CursorId: nextCursor,
				NextPage: func(ctx context.Context) (*qdata.PageResult[*qdata.Entity], error) {
					if nextCursor < 0 {
						return &qdata.PageResult[*qdata.Entity]{
							Items:    []*qdata.Entity{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					// Create new request with updated cursor
					newArgs := append(otherArgs,
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursor))
					// Also pass along the type hints
					for _, typeHintOpt := range typeHintOpts {
						newArgs = append(newArgs, typeHintOpt)
					}
					return me.PrepareQuery(sql, newArgs...).NextPage(ctx)
				},
			}, nil
		},
	}
}

func (me *NatsStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) bool {
	msg := &qprotobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId.AsString(),
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

		if writer == nil || writer.IsEmpty() {
			wr := new(qdata.EntityId).FromString("")

			appName := qcontext.GetAppName(ctx)
			if me.clientId == nil && appName != "" {
				iterator := me.PrepareQuery("SELECT Name FROM Client WHERE Name = %q", appName)

				for iterator.Next(ctx) {
					me.clientId = &iterator.Get().EntityId
				}
			}

			if me.clientId != nil {
				*wr = *me.clientId
			}

			r.WriterId = wr
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
		EntityType: entityType.AsString(),
		FieldName:  fieldType.AsString(),
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
		Type: entityType.AsString(),
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

	return new(qdata.EntitySchema).FromEntitySchemaPb(response.Schema)
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

func (me *NatsStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return me.publishSig
}

func (me *NatsStoreInteractor) InitializeSchema(ctx context.Context) {
	// No-op for NATS
}

func (me *NatsStoreInteractor) CreateSnapshot(ctx context.Context) *qdata.Snapshot {
	msg := &qprotobufs.ApiConfigCreateSnapshotRequest{}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		qlog.Error("Failed to create snapshot: %v", err)
		return nil
	}

	var response qprotobufs.ApiConfigCreateSnapshotResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		qlog.Error("Failed to create snapshot: error while unmarshalling response: %v", err)
		return nil
	}

	if response.Status != qprotobufs.ApiConfigCreateSnapshotResponse_SUCCESS {
		qlog.Error("Failed to create snapshot: response returned an unsuccessful status: %v", response.Status)
		return nil
	}

	return new(qdata.Snapshot).FromSnapshotPb(response.Snapshot)
}

func (me *NatsStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) {
	msg := &qprotobufs.ApiConfigRestoreSnapshotRequest{
		Snapshot: ss.AsSnapshotPb(),
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to restore snapshot: %v", err)
		return
	}

	var response qprotobufs.ApiConfigRestoreSnapshotResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		qlog.Error("Failed to restore snapshot: error while unmarshalling response: %v", err)
		return
	}

	if response.Status != qprotobufs.ApiConfigRestoreSnapshotResponse_SUCCESS {
		qlog.Error("Failed to restore snapshot: response returned an unsuccessful status: %v", response.Status)
	}
}
