package store

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WebConfig struct {
	Address string
}

type responseCtxPair struct {
	ctx context.Context
	ch  chan *protobufs.WebMessage
}

type Web struct {
	config           WebConfig
	client           web.Client
	callbacks        map[string][]data.NotificationCallback
	pendingResponses map[string]responseCtxPair
	mu               sync.RWMutex
}

func NewWeb(config WebConfig) data.Store {
	return &Web{
		config:           config,
		callbacks:        map[string][]data.NotificationCallback{},
		pendingResponses: map[string]responseCtxPair{},
	}
}

func (s *Web) Connect(ctx context.Context) {
	s.Disconnect(ctx)

	log.Info("Connecting to %v", s.config.Address)

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(s.config.Address, nil)
	if err != nil {
		log.Error("Failed to connect: %v", err)
		return
	}

	s.client = web.NewClient(conn, func(id string) {
		log.Info("Connection closed: %v", id)
		s.Disconnect(ctx)
	})

	s.client.SetMessageHandler(func(_ web.Client, msg web.Message) {
		s.handleMessage(msg)
	})
}

func (s *Web) Disconnect(ctx context.Context) {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *Web) IsConnected(ctx context.Context) bool {
	if s.client == nil {
		return false
	}

	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeGetDatabaseConnectionStatusRequest{})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return false
	}

	var resp protobufs.WebRuntimeGetDatabaseConnectionStatusResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Connected
}

func (s *Web) CreateSnapshot(ctx context.Context) data.Snapshot {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigCreateSnapshotRequest{})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.WebConfigCreateSnapshotResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigCreateSnapshotResponse_SUCCESS {
		return nil
	}

	return snapshot.FromPb(resp.Snapshot)
}

func (s *Web) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigRestoreSnapshotRequest{
		Snapshot: snapshot.ToPb(ss),
	})

	s.sendAndWait(ctx, msg)
}

func (s *Web) CreateEntity(ctx context.Context, entityType, parentId, name string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
		Name:     name,
	})

	s.sendAndWait(ctx, msg)
}

func (s *Web) GetEntity(ctx context.Context, entityId string) data.Entity {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntityRequest{
		Id: entityId,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.WebConfigGetEntityResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigGetEntityResponse_SUCCESS {
		return nil
	}

	return entity.FromEntityPb(resp.Entity)
}

func (s *Web) SetEntity(ctx context.Context, e data.Entity) {
	// Set entity is handled through CreateEntity
}

func (s *Web) DeleteEntity(ctx context.Context, entityId string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigDeleteEntityRequest{
		Id: entityId,
	})

	s.sendAndWait(ctx, msg)
}

func (s *Web) FindEntities(ctx context.Context, entityType string) []string {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeGetEntitiesRequest{
		EntityType: entityType,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.WebRuntimeGetEntitiesResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	ids := make([]string, len(resp.Entities))
	for i, e := range resp.Entities {
		ids[i] = e.Id
	}
	return ids
}

func (s *Web) GetEntityTypes(ctx context.Context) []string {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntityTypesRequest{})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.WebConfigGetEntityTypesResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	return resp.Types
}

func (s *Web) EntityExists(ctx context.Context, entityId string) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeEntityExistsRequest{
		EntityId: entityId,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return false
	}

	var resp protobufs.WebRuntimeEntityExistsResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Exists
}

func (s *Web) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return false
	}

	var resp protobufs.WebRuntimeFieldExistsResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Exists
}

func (s *Web) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntitySchemaRequest{
		Type: entityType,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.WebConfigGetEntitySchemaResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigGetEntitySchemaResponse_SUCCESS {
		return nil
	}

	return entity.FromSchemaPb(resp.Schema)
}

func (s *Web) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigSetEntitySchemaRequest{
		Schema: entity.ToSchemaPb(schema),
	})

	s.sendAndWait(ctx, msg)
}

func (s *Web) GetFieldSchema(ctx context.Context, fieldName, entityType string) data.FieldSchema {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntitySchemaRequest{
		Type: entityType,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return nil
	}

	var resp protobufs.WebConfigGetEntitySchemaResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigGetEntitySchemaResponse_SUCCESS {
		return nil
	}

	for _, f := range resp.Schema.GetFields() {
		if f.GetName() == fieldName {
			return field.FromSchemaPb(f)
		}
	}

	return nil
}

func (s *Web) SetFieldSchema(ctx context.Context, fieldName, entityType string, schema data.FieldSchema) {
	entitySchema := s.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		log.Error("Failed to get entity schema")
		return
	}

	fields := entitySchema.GetFields()
	newFields := make([]data.FieldSchema, 0, len(fields))
	updated := false
	for _, f := range fields {
		if f.GetFieldName() == fieldName {
			newFields = append(newFields, schema)
			updated = true
		} else {
			newFields = append(newFields, f)
		}
	}

	if !updated {
		newFields = append(newFields, schema)
	}

	entitySchema.SetFields(newFields)
	s.SetEntitySchema(ctx, entitySchema)
}

func (s *Web) Read(ctx context.Context, requests ...data.Request) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}

	dbRequests := make([]*protobufs.DatabaseRequest, len(requests))
	for i, r := range requests {
		dbRequests[i] = request.ToPb(r)
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeDatabaseRequest{
		RequestType: protobufs.WebRuntimeDatabaseRequest_READ,
		Requests:    dbRequests,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return
	}

	var resp protobufs.WebRuntimeDatabaseResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return
	}

	for i, r := range resp.Response {
		if i >= len(requests) {
			break
		}
		requests[i].SetValue(field.FromAnyPb(r.Value))
		requests[i].SetSuccessful(r.Success)
		if r.WriteTime != nil {
			wt := r.WriteTime.Raw.AsTime()
			requests[i].SetWriteTime(&wt)
		}
		if r.WriterId != nil {
			wr := r.WriterId.Raw
			requests[i].SetWriter(&wr)
		}
	}
}

func (s *Web) Write(ctx context.Context, requests ...data.Request) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}

	dbRequests := make([]*protobufs.DatabaseRequest, len(requests))
	for i, r := range requests {
		dbRequests[i] = request.ToPb(r)
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeDatabaseRequest{
		RequestType: protobufs.WebRuntimeDatabaseRequest_WRITE,
		Requests:    dbRequests,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return
	}

	var resp protobufs.WebRuntimeDatabaseResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return
	}

	for i, r := range resp.Response {
		if i >= len(requests) {
			break
		}
		requests[i].SetSuccessful(r.Success)
	}
}

func (s *Web) handleMessage(msg web.Message) {
	s.mu.Lock()
	if rsp, ok := s.pendingResponses[msg.Header.Id]; ok {
		delete(s.pendingResponses, msg.Header.Id)
		s.mu.Unlock()
		select {
		case <-rsp.ctx.Done():
			log.Info("Context done")
		case rsp.ch <- msg:
			log.Trace("Returned response for headerId=(%s)", msg.Header.Id)
		}
		close(rsp.ch)
		return
	}
	s.mu.Unlock()
}

func (s *Web) sendAndWait(ctx context.Context, msg web.Message) web.Message {
	if s.client == nil {
		log.Error("Not connected")
		return nil
	}

	// Generate unique request ID
	requestId := uuid.New().String()
	msg.Header.Id = requestId
	msg.Header.Timestamp = timestamppb.Now()

	responseCh := make(chan web.Message, 1)

	s.mu.Lock()
	s.pendingResponses[requestId] = responseCtxPair{
		ctx: ctx,
		ch:  responseCh,
	}
	s.mu.Unlock()

	s.client.Write(msg)

	select {
	case <-ctx.Done():
		log.Warn("Context done")
		return nil
	case response := <-responseCh:
		return response
	case <-time.After(10 * time.Second):
		log.Error("Timeout waiting for response to %v", requestId)
		s.mu.Lock()
		delete(s.pendingResponses, requestId)
		s.mu.Unlock()
		return nil
	}
}

func (s *Web) Notify(ctx context.Context, config data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeRegisterNotificationRequest{
		Requests: []*protobufs.DatabaseNotificationConfig{notification.ToConfigPb(config)},
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return notification.NewToken("", s, nil)
	}

	var resp protobufs.WebRuntimeRegisterNotificationResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return notification.NewToken("", s, nil)
	}

	if len(resp.Tokens) == 0 {
		return notification.NewToken("", s, nil)
	}

	token := resp.Tokens[0]
	s.callbacks[token] = append(s.callbacks[token], cb)

	return notification.NewToken(token, s, cb)
}

func (s *Web) Unnotify(ctx context.Context, token string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	})

	s.sendAndWait(ctx, msg)
	delete(s.callbacks, token)
}

func (s *Web) UnnotifyCallback(ctx context.Context, token string, cb data.NotificationCallback) {
	if s.callbacks[token] == nil {
		return
	}

	callbacks := []data.NotificationCallback{}
	for _, callback := range s.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	if len(callbacks) == 0 {
		s.Unnotify(ctx, token)
	} else {
		s.callbacks[token] = callbacks
	}
}

func (s *Web) ProcessNotifications(ctx context.Context) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeGetNotificationsRequest{})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return
	}

	var resp protobufs.WebRuntimeGetNotificationsResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return
	}

	for _, n := range resp.Notifications {
		notification := notification.FromPb(n)
		if callbacks, ok := s.callbacks[notification.GetToken()]; ok {
			for _, cb := range callbacks {
				cb.Fn(ctx, notification)
			}
		}
	}
}

func (s *Web) TriggerNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	// Not supported
}
