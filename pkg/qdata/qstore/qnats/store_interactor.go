package qnats

import (
	"context"
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
)

const DefaultRequestTimeout = 5 * time.Second

type NatsStoreInteractor struct {
	core          NatsCore
	publishSig    qss.Signal[qdata.PublishNotificationArgs]
	readEventSig  qss.Signal[qdata.ReadEventArgs]
	writeEventSig qss.Signal[qdata.WriteEventArgs]
	clientId      *qdata.EntityId
}

func NewStoreInteractor(core NatsCore) qdata.StoreInteractor {
	return &NatsStoreInteractor{
		core:          core,
		publishSig:    qss.New[qdata.PublishNotificationArgs](),
		readEventSig:  qss.New[qdata.ReadEventArgs](),
		writeEventSig: qss.New[qdata.WriteEventArgs](),
	}
}

func (me *NatsStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) (*qdata.Entity, error) {
	msg := &qprotobufs.ApiConfigCreateEntityRequest{
		Type:     entityType.AsString(),
		ParentId: parentId.AsString(),
		Name:     name,
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetWriteSubject(),
		msg)
	if err != nil {
		return nil, err
	}

	var response qprotobufs.ApiConfigCreateEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil, err
	}

	if response.Status != qprotobufs.ApiConfigCreateEntityResponse_SUCCESS {
		return nil, fmt.Errorf("failed to create entity: %s", response.Status.String())
	}

	return new(qdata.Entity).Init(qdata.EntityId(response.Id)), nil
}

func (me *NatsStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) error {
	msg := &qprotobufs.ApiConfigDeleteEntityRequest{
		Id: entityId.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	_, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetWriteSubject(),
		msg)
	if err != nil {
		return err
	}
	return nil
}

func (me *NatsStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityId], error) {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	result := &qdata.PageResult[qdata.EntityId]{
		Items:    []qdata.EntityId{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
			msg := &qprotobufs.ApiRuntimeFindEntitiesRequest{
				EntityType: entityType.AsString(),
				PageSize:   pageConfig.PageSize,
				Cursor:     pageConfig.CursorId,
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
			defer cancel()
			resp, err := me.core.Request(
				timeoutCtx,
				me.core.GetKeyGenerator().GetReadSubject(),
				msg)
			if err != nil {
				return nil, err
			}

			var response qprotobufs.ApiRuntimeFindEntitiesResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, err
			}

			if response.Status != qprotobufs.ApiRuntimeFindEntitiesResponse_SUCCESS {
				return nil, fmt.Errorf("failed to find entities: %s", response.Status.String())
			}

			entities := make([]qdata.EntityId, 0, len(response.Entities))
			for _, id := range response.Entities {
				entities = append(entities, qdata.EntityId(id))
			}

			nextCursor := response.NextCursor

			qlog.Trace("Found %d entities of type %s, nextCursor: %d", len(entities), entityType, nextCursor)

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
						qdata.POCursorId(nextCursor))
				},
			}, nil
		},
	}
	return result, nil
}

func (me *NatsStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityType], error) {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	result := &qdata.PageResult[qdata.EntityType]{
		Items:    []qdata.EntityType{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
			msg := &qprotobufs.ApiRuntimeGetEntityTypesRequest{
				PageSize: pageConfig.PageSize,
				Cursor:   pageConfig.CursorId,
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
			defer cancel()
			resp, err := me.core.Request(
				timeoutCtx,
				me.core.GetKeyGenerator().GetReadSubject(),
				msg)
			if err != nil {
				return nil, err
			}

			var response qprotobufs.ApiRuntimeGetEntityTypesResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, err
			}

			if response.Status != qprotobufs.ApiRuntimeGetEntityTypesResponse_SUCCESS {
				return nil, fmt.Errorf("failed to get entity types: %s", response.Status.String())
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
						qdata.POCursorId(nextCursor))
				},
			}, nil
		},
	}
	return result, nil
}

func (me *NatsStoreInteractor) PrepareQuery(sql string, args ...any) (*qdata.PageResult[qdata.QueryRow], error) {
	pageOpts := []qdata.PageOpts{}
	typeHintOpts := []qdata.TypeHintOpts{}
	queryEngine := qdata.QEExprLang
	otherArgs := []any{}

	for _, arg := range args {
		switch arg := arg.(type) {
		case qdata.PageOpts:
			pageOpts = append(pageOpts, arg)
		case qdata.TypeHintOpts:
			typeHintOpts = append(typeHintOpts, arg)
		case qdata.QueryEngineType:
			queryEngine = arg
		default:
			otherArgs = append(otherArgs, arg)
		}
	}

	formattedQuery := fmt.Sprintf(sql, otherArgs...)
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	typeHints := []*qprotobufs.TypeHint{}
	typeHintMap := make(qdata.TypeHintMap)
	for _, opt := range typeHintOpts {
		opt(typeHintMap)
	}

	for fieldType, valueType := range typeHintMap {
		typeHints = append(typeHints, &qprotobufs.TypeHint{
			FieldType: fieldType,
			ValueType: valueType.AsString(),
		})
	}

	result := &qdata.PageResult[qdata.QueryRow]{
		Items:    []qdata.QueryRow{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.QueryRow], error) {
			msg := &qprotobufs.ApiRuntimeQueryRequest{
				Query:     formattedQuery,
				PageSize:  pageConfig.PageSize,
				Cursor:    pageConfig.CursorId,
				TypeHints: typeHints,
				Engine:    string(queryEngine),
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
			defer cancel()
			resp, err := me.core.Request(
				timeoutCtx,
				me.core.GetKeyGenerator().GetReadSubject(),
				msg)
			if err != nil {
				return nil, fmt.Errorf("failed to execute query: %v", err)
			}

			var response qprotobufs.ApiRuntimeQueryResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, fmt.Errorf("failed to parse query response: %v", err)
			}

			if response.Status != qprotobufs.ApiRuntimeQueryResponse_SUCCESS {
				return nil, fmt.Errorf("query failed: %s", response.Status.String())
			}

			rows := make([]qdata.QueryRow, 0, len(response.Rows))
			for _, rowPb := range response.Rows {
				row := qdata.NewQueryRow()
				row.FromQueryRowPb(rowPb)
				rows = append(rows, row)
			}

			nextCursor := response.NextCursor

			return &qdata.PageResult[qdata.QueryRow]{
				Items:    rows,
				CursorId: nextCursor,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.QueryRow], error) {
					if nextCursor < 0 {
						return &qdata.PageResult[qdata.QueryRow]{
							Items:    []qdata.QueryRow{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					newArgs := append(otherArgs,
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursor),
						queryEngine)
					for _, typeHintOpt := range typeHintOpts {
						newArgs = append(newArgs, typeHintOpt)
					}
					return me.PrepareQuery(sql, newArgs...)
				},
			}, nil
		},
	}
	return result, nil
}

func (me *NatsStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) (bool, error) {
	msg := &qprotobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetReadSubject(),
		msg)
	if err != nil {
		return false, err
	}

	var response qprotobufs.ApiRuntimeEntityExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false, err
	}

	if response.Status != qprotobufs.ApiRuntimeEntityExistsResponse_SUCCESS {
		return false, fmt.Errorf("failed to check entity existence: %s", response.Status.String())
	}

	return response.Exists, nil
}

func (me *NatsStoreInteractor) Read(ctx context.Context, requests ...*qdata.Request) error {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_READ,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = r.AsRequestPb()
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetReadSubject(),
		msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS {
		return fmt.Errorf("read failed: %s", response.Status.String())
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

		if r.Success {
			me.readEventSig.Emit(qdata.ReadEventArgs{
				Ctx: ctx,
				Req: requests[i],
			})
		}
	}

	return nil
}

func (me *NatsStoreInteractor) Write(ctx context.Context, requests ...*qdata.Request) error {
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
				page, err := me.PrepareQuery(`SELECT "$EntityId" FROM Client WHERE Name == %q`, appName)
				if err == nil {
					page.ForEach(ctx, func(client qdata.QueryRow) bool {
						entityId := client.AsEntity().EntityId
						me.clientId = &entityId
						return false
					})
				}
			}

			if me.clientId != nil {
				*wr = *me.clientId
			}

			r.WriterId = wr
		}

		msg.Requests[i] = r.AsRequestPb()
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetWriteSubject(),
		msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiRuntimeDatabaseResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiRuntimeDatabaseResponse_SUCCESS {
		return fmt.Errorf("write failed: %s", response.Status.String())
	}

	for i, r := range response.Response {
		if i >= len(requests) {
			break
		}
		requests[i].Success = r.Success

		if r.Success {
			me.writeEventSig.Emit(qdata.WriteEventArgs{
				Ctx: ctx,
				Req: requests[i],
			})
		}
	}

	return nil
}

func (me *NatsStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (bool, error) {
	msg := &qprotobufs.ApiRuntimeFieldExistsRequest{
		EntityType: entityType.AsString(),
		FieldName:  fieldType.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetReadSubject(),
		msg)
	if err != nil {
		return false, err
	}

	var response qprotobufs.ApiRuntimeFieldExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false, err
	}

	if response.Status != qprotobufs.ApiRuntimeFieldExistsResponse_SUCCESS {
		return false, fmt.Errorf("failed to check field existence: %s", response.Status.String())
	}

	return response.Exists, nil
}

func (me *NatsStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) (*qdata.EntitySchema, error) {
	msg := &qprotobufs.ApiConfigGetEntitySchemaRequest{
		Type: entityType.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetReadSubject(),
		msg)
	if err != nil {
		return nil, err
	}

	var response qprotobufs.ApiConfigGetEntitySchemaResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil, err
	}

	if response.Status != qprotobufs.ApiConfigGetEntitySchemaResponse_SUCCESS {
		return nil, fmt.Errorf("failed to get entity schema: %v", response.Status)
	}

	return new(qdata.EntitySchema).FromEntitySchemaPb(response.Schema), nil
}

func (me *NatsStoreInteractor) SetEntitySchema(ctx context.Context, schema *qdata.EntitySchema) error {
	msg := &qprotobufs.ApiConfigSetEntitySchemaRequest{
		Schema: schema.AsEntitySchemaPb(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultRequestTimeout)
	defer cancel()
	resp, err := me.core.Request(
		timeoutCtx,
		me.core.GetKeyGenerator().GetWriteSubject(),
		msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiConfigSetEntitySchemaResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiConfigSetEntitySchemaResponse_SUCCESS {
		return fmt.Errorf("failed to set entity schema: %v", response.Status)
	}

	return nil
}

func (me *NatsStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (*qdata.FieldSchema, error) {
	schema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil || schema == nil {
		return nil, err
	}

	return schema.Fields[fieldType], nil
}

func (me *NatsStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) error {
	entitySchema, err := me.GetEntitySchema(ctx, entityType)
	if err != nil || entitySchema == nil {
		return fmt.Errorf("failed to get entity schema for type %s", entityType)
	}

	entitySchema.Fields[fieldType] = schema

	return me.SetEntitySchema(ctx, entitySchema)
}

func (me *NatsStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return me.publishSig
}

func (me *NatsStoreInteractor) InitializeSchema(ctx context.Context) error {
	// No-op for NATS
	return nil
}

func (me *NatsStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	msg := &qprotobufs.ApiConfigCreateSnapshotRequest{}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil, err
	}

	var response qprotobufs.ApiConfigCreateSnapshotResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil, err
	}

	if response.Status != qprotobufs.ApiConfigCreateSnapshotResponse_SUCCESS {
		return nil, fmt.Errorf("failed to create snapshot: %v", response.Status)
	}

	return new(qdata.Snapshot).FromSnapshotPb(response.Snapshot), nil
}

func (me *NatsStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	msg := &qprotobufs.ApiConfigRestoreSnapshotRequest{
		Snapshot: ss.AsSnapshotPb(),
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiConfigRestoreSnapshotResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiConfigRestoreSnapshotResponse_SUCCESS {
		return fmt.Errorf("failed to restore snapshot: %v", response.Status)
	}

	return nil
}

func (me *NatsStoreInteractor) ReadEvent() qss.Signal[qdata.ReadEventArgs] {
	return me.readEventSig
}

func (me *NatsStoreInteractor) WriteEvent() qss.Signal[qdata.WriteEventArgs] {
	return me.writeEventSig
}
