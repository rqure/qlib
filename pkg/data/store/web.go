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

type Web struct {
	config           WebConfig
	client           web.Client
	callbacks        map[string][]data.NotificationCallback
	pendingResponses map[string]chan *protobufs.WebMessage
	mu               sync.RWMutex
}

func NewWeb(config WebConfig) data.Store {
	return &Web{
		config:           config,
		callbacks:        map[string][]data.NotificationCallback{},
		pendingResponses: map[string]chan *protobufs.WebMessage{},
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
	})

	s.client.SetMessageHandler(func(_ web.Client, msg web.Message) {
		s.handleMessage(ctx, msg)
	})
}

func (s *Web) Disconnect(ctx context.Context) {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *Web) IsConnected(context.Context) bool {
	return s.client != nil
}

func (s *Web) CreateSnapshot(ctx context.Context) data.Snapshot {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigCreateSnapshotRequest{})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
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

func (s *Web) handleMessage(ctx context.Context, msg web.Message) {
	s.mu.Lock()
	if ch, ok := s.pendingResponses[msg.Header.Id]; ok {
		delete(s.pendingResponses, msg.Header.Id)
		s.mu.Unlock()
		select {
		case <-ctx.Done():
		case ch <- msg:
		}
		close(ch)
		return
	}
	s.mu.Unlock()
}

func (s *Web) sendAndWait(ctx context.Context, msg web.Message) web.Message {
	if !s.IsConnected(ctx) {
		log.Error("Not connected")
		return nil
	}

	// Generate unique request ID
	requestId := uuid.New().String()
	msg.Header.Id = requestId
	msg.Header.Timestamp = timestamppb.Now()

	responseCh := make(chan web.Message, 1)

	s.mu.Lock()
	s.pendingResponses[requestId] = responseCh
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

// Implement remaining data.Store interface methods for temp storage and sorted sets
func (s *Web) TempSet(ctx context.Context, key, value string, expiration time.Duration) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempSetRequest{
		Key:          key,
		Value:        value,
		ExpirationMs: expiration.Milliseconds(),
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		return false
	}

	var resp protobufs.WebRuntimeTempSetResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Success
}

func (s *Web) TempGet(ctx context.Context, key string) string {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempGetRequest{
		Key: key,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		return ""
	}

	var resp protobufs.WebRuntimeTempGetResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return ""
	}

	return resp.Value
}

func (s *Web) TempExpire(ctx context.Context, key string, expiration time.Duration) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempExpireRequest{
		Key:          key,
		ExpirationMs: expiration.Milliseconds(),
	})

	s.sendAndWait(ctx, msg)
}

func (s *Web) TempDel(ctx context.Context, key string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempDelRequest{
		Key: key,
	})

	s.sendAndWait(ctx, msg)
}

func (s *Web) SortedSetAdd(ctx context.Context, key string, member string, score float64) int64 {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetAddRequest{
		Key:    key,
		Member: member,
		Score:  score,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		return 0
	}

	var resp protobufs.WebRuntimeSortedSetAddResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return 0
	}

	return resp.Result
}

func (s *Web) SortedSetRemove(ctx context.Context, key string, member string) int64 {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetRemoveRequest{
		Key:    key,
		Member: member,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		return 0
	}

	var resp protobufs.WebRuntimeSortedSetRemoveResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return 0
	}

	return resp.Result
}

func (s *Web) SortedSetRemoveRangeByRank(ctx context.Context, key string, start, stop int64) int64 {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetRemoveRangeByRankRequest{
		Key:   key,
		Start: start,
		Stop:  stop,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		return 0
	}

	var resp protobufs.WebRuntimeSortedSetRemoveRangeByRankResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return 0
	}

	return resp.Result
}

func (s *Web) SortedSetRangeByScoreWithScores(ctx context.Context, key string, min, max string) []data.SortedSetMember {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetRangeByScoreWithScoresRequest{
		Key: key,
		Min: min,
		Max: max,
	})

	response := s.sendAndWait(ctx, msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebRuntimeSortedSetRangeByScoreWithScoresResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	members := make([]data.SortedSetMember, len(resp.Members))
	for i, m := range resp.Members {
		members[i] = data.SortedSetMember{
			Member: m.Member,
			Score:  m.Score,
		}
	}

	return members
}
