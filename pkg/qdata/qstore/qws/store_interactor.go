package qws

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

const DefaultOperationTimeout = 5 * time.Second

// WebSocketStoreInteractor implements StoreInteractor using WebSocket
type WebSocketStoreInteractor struct {
	core                   WebSocketCore
	publishSig             qss.Signal[qdata.PublishNotificationArgs]
	readEventSig           qss.Signal[qdata.ReadEventArgs]
	writeEventSig          qss.Signal[qdata.WriteEventArgs]
	interactorConnected    qss.Signal[qdata.ConnectedArgs]
	interactorDisconnected qss.Signal[qdata.DisconnectedArgs]
	clientId               *qdata.EntityId
}

// NewStoreInteractor creates a new WebSocketStoreInteractor
func NewStoreInteractor(core WebSocketCore) qdata.StoreInteractor {
	ws := &WebSocketStoreInteractor{
		core:                   core,
		publishSig:             qss.New[qdata.PublishNotificationArgs](),
		readEventSig:           qss.New[qdata.ReadEventArgs](),
		writeEventSig:          qss.New[qdata.WriteEventArgs](),
		interactorConnected:    qss.New[qdata.ConnectedArgs](),
		interactorDisconnected: qss.New[qdata.DisconnectedArgs](),
	}

	ws.core.Connected().Connect(ws.onConnected)
	ws.core.Disconnected().Connect(ws.onDisconnected)

	return ws
}

func (si *WebSocketStoreInteractor) onConnected(args qdata.ConnectedArgs) {
	si.interactorConnected.Emit(args)
}

func (si *WebSocketStoreInteractor) onDisconnected(args qdata.DisconnectedArgs) {
	si.interactorDisconnected.Emit(args)
}

func (si *WebSocketStoreInteractor) InteractorConnected() qss.Signal[qdata.ConnectedArgs] {
	return si.interactorConnected
}

func (si *WebSocketStoreInteractor) InteractorDisconnected() qss.Signal[qdata.DisconnectedArgs] {
	return si.interactorDisconnected
}

// CreateEntity creates a new entity
func (si *WebSocketStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) (*qdata.Entity, error) {
	msg := &qprotobufs.ApiConfigCreateEntityRequest{
		Type:     entityType.AsString(),
		ParentId: parentId.AsString(),
		Name:     name,
	}

	// Use explicit timeout for this operation
	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()

	resp, err := si.core.Request(
		timeoutCtx,
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

// DeleteEntity deletes an entity
func (si *WebSocketStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) error {
	msg := &qprotobufs.ApiConfigDeleteEntityRequest{
		Id: entityId.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()
	resp, err := si.core.Request(
		timeoutCtx,
		msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiConfigDeleteEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if response.Status != qprotobufs.ApiConfigDeleteEntityResponse_SUCCESS {
		return fmt.Errorf("failed to delete entity: %s", response.Status.String())
	}

	return nil
}

// PrepareQuery prepares and executes a query
func (si *WebSocketStoreInteractor) PrepareQuery(sql string, args ...any) (*qdata.PageResult[qdata.QueryRow], error) {
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

			// Set an explicit timeout for query operations
			timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
			defer cancel()

			startTime := time.Now()
			resp, err := si.core.Request(
				timeoutCtx,
				msg)
			if err != nil {
				return nil, fmt.Errorf("failed to execute query: %v", err)
			}
			qlog.Trace("Query request took %s", time.Since(startTime))

			startTime = time.Now()
			var response qprotobufs.ApiRuntimeQueryResponse
			if err := resp.Payload.UnmarshalTo(&response); err != nil {
				return nil, fmt.Errorf("failed to parse query response: %v", err)
			}
			qlog.Trace("Query response unmarshal parsing took %s", time.Since(startTime))

			if response.Status != qprotobufs.ApiRuntimeQueryResponse_SUCCESS {
				return nil, fmt.Errorf("query failed: %s", response.Status.String())
			}

			rows := make([]qdata.QueryRow, 0, len(response.Rows))
			startTime = time.Now()
			for _, rowPb := range response.Rows {
				row := qdata.NewQueryRow()
				row.FromQueryRowPb(rowPb)
				rows = append(rows, row)
			}
			qlog.Trace("Query row conversion from Pb took %s", time.Since(startTime))

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
					return si.PrepareQuery(sql, newArgs...)
				},
			}, nil
		},
	}
	return result, nil
}

// FindEntities finds entities of a specific type
func (si *WebSocketStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityId], error) {
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

			timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
			defer cancel()
			resp, err := si.core.Request(
				timeoutCtx,
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
					return si.FindEntities(entityType,
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursor))
				},
			}, nil
		},
	}
	return result, nil
}

// GetEntityTypes gets all entity types
func (si *WebSocketStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) (*qdata.PageResult[qdata.EntityType], error) {
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

			timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
			defer cancel()
			resp, err := si.core.Request(
				timeoutCtx,
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
					return si.GetEntityTypes(
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursor))
				},
			}, nil
		},
	}
	return result, nil
}

// EntityExists checks if an entity exists
func (si *WebSocketStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) (bool, error) {
	msg := &qprotobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()
	resp, err := si.core.Request(
		timeoutCtx,
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

// FieldExists checks if a field exists
func (si *WebSocketStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (bool, error) {
	msg := &qprotobufs.ApiRuntimeFieldExistsRequest{
		EntityType: entityType.AsString(),
		FieldName:  fieldType.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()
	resp, err := si.core.Request(
		timeoutCtx,
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

// GetEntitySchema gets the schema for an entity type
func (si *WebSocketStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) (*qdata.EntitySchema, error) {
	msg := &qprotobufs.ApiConfigGetEntitySchemaRequest{
		Type: entityType.AsString(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()
	resp, err := si.core.Request(
		timeoutCtx,
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

// SetEntitySchema sets the schema for an entity type
func (si *WebSocketStoreInteractor) SetEntitySchema(ctx context.Context, schema *qdata.EntitySchema) error {
	msg := &qprotobufs.ApiConfigSetEntitySchemaRequest{
		Schema: schema.AsEntitySchemaPb(),
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()
	resp, err := si.core.Request(
		timeoutCtx,
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

// GetFieldSchema gets the schema for a field
func (si *WebSocketStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) (*qdata.FieldSchema, error) {
	schema, err := si.GetEntitySchema(ctx, entityType)
	if err != nil || schema == nil {
		return nil, err
	}

	return schema.Fields[fieldType], nil
}

// SetFieldSchema sets the schema for a field
func (si *WebSocketStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) error {
	entitySchema, err := si.GetEntitySchema(ctx, entityType)
	if err != nil || entitySchema == nil {
		return fmt.Errorf("failed to get entity schema for type %s", entityType)
	}

	entitySchema.Fields[fieldType] = schema

	return si.SetEntitySchema(ctx, entitySchema)
}

// Read reads one or more fields
func (si *WebSocketStoreInteractor) Read(ctx context.Context, requests ...*qdata.Request) error {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_READ,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		msg.Requests[i] = r.AsRequestPb()
	}

	// Set an explicit timeout for read operations
	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()

	resp, err := si.core.Request(
		timeoutCtx,
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
			si.readEventSig.Emit(qdata.ReadEventArgs{
				Ctx: ctx,
				Req: requests[i],
			})
		}
	}

	return nil
}

// Write writes one or more fields
func (si *WebSocketStoreInteractor) Write(ctx context.Context, requests ...*qdata.Request) error {
	msg := &qprotobufs.ApiRuntimeDatabaseRequest{
		RequestType: qprotobufs.ApiRuntimeDatabaseRequest_WRITE,
		Requests:    make([]*qprotobufs.DatabaseRequest, len(requests)),
	}

	for i, r := range requests {
		writer := r.WriterId

		if writer == nil || writer.IsEmpty() {
			wr := new(qdata.EntityId).FromString("")

			appName := qcontext.GetAppName(ctx)
			if si.clientId == nil && appName != "" {
				page, err := si.PrepareQuery(`SELECT "$EntityId" FROM Client WHERE Name == %q`, appName)
				if err == nil {
					page.ForEach(ctx, func(client qdata.QueryRow) bool {
						entityId := client.AsEntity().EntityId
						si.clientId = &entityId
						return false
					})
				}
			}

			if si.clientId != nil {
				*wr = *si.clientId
			}

			r.WriterId = wr
		}

		msg.Requests[i] = r.AsRequestPb()
	}

	// Set an explicit timeout for write operations
	timeoutCtx, cancel := context.WithTimeout(ctx, DefaultOperationTimeout)
	defer cancel()

	resp, err := si.core.Request(
		timeoutCtx,
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
			si.writeEventSig.Emit(qdata.WriteEventArgs{
				Ctx: ctx,
				Req: requests[i],
			})
		}
	}

	return nil
}

// InitializeSchema initializes the database schema
func (si *WebSocketStoreInteractor) InitializeSchema(ctx context.Context) error {
	// No-op for WebSocket, similar to NATS implementation
	return nil
}

// CreateSnapshot creates a database snapshot
func (si *WebSocketStoreInteractor) CreateSnapshot(ctx context.Context) (*qdata.Snapshot, error) {
	msg := &qprotobufs.ApiConfigCreateSnapshotRequest{}

	resp, err := si.core.Request(ctx, msg)
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

// RestoreSnapshot restores a database from a snapshot
func (si *WebSocketStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) error {
	msg := &qprotobufs.ApiConfigRestoreSnapshotRequest{
		Snapshot: ss.AsSnapshotPb(),
	}

	resp, err := si.core.Request(ctx, msg)
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

// PublishNotifications returns a signal for notification publications
func (si *WebSocketStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return si.publishSig
}

// ReadEvent returns a signal for read events
func (si *WebSocketStoreInteractor) ReadEvent() qss.Signal[qdata.ReadEventArgs] {
	return si.readEventSig
}

// WriteEvent returns a signal for write events
func (si *WebSocketStoreInteractor) WriteEvent() qss.Signal[qdata.WriteEventArgs] {
	return si.writeEventSig
}
