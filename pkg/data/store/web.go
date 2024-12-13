package store

import (
	"sync"
	"time"

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
)

type WebConfig struct {
	Address string
}

type Web struct {
	config             WebConfig
	client             web.Client
	callbacks          map[string][]data.NotificationCallback
	pendingResponses   map[string]chan *protobufs.WebMessage
	mu                 sync.RWMutex
	notifications      []data.Notification
	notificationsMutex sync.Mutex
}

func NewWeb(config WebConfig) data.Store {
	return &Web{
		config:           config,
		callbacks:        map[string][]data.NotificationCallback{},
		pendingResponses: map[string]chan *protobufs.WebMessage{},
	}
}

func (s *Web) Connect() {
	s.Disconnect()

	log.Info("[Web::Connect] Connecting to %v", s.config.Address)

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(s.config.Address, nil)
	if err != nil {
		log.Error("[Web::Connect] Failed to connect: %v", err)
		return
	}

	s.client = web.NewClient(conn, func(id string) {
		log.Info("[Web::Connect] Connection closed: %v", id)
	})

	s.client.SetMessageHandler(func(_ web.Client, msg web.Message) {
		s.handleMessage(msg)
	})
}

func (s *Web) Disconnect() {
	if s.client != nil {
		s.client.Close()
		s.client = nil
	}
}

func (s *Web) IsConnected() bool {
	return s.client != nil
}

func (s *Web) CreateSnapshot() data.Snapshot {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "create_snapshot",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigCreateSnapshotRequest{})

	response := s.sendAndWait(msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebConfigCreateSnapshotResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::CreateSnapshot] Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigCreateSnapshotResponse_SUCCESS {
		return nil
	}

	return snapshot.FromPb(resp.Snapshot)
}

func (s *Web) RestoreSnapshot(ss data.Snapshot) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "restore_snapshot",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigRestoreSnapshotRequest{
		Snapshot: snapshot.ToPb(ss),
	})

	s.sendAndWait(msg)
}

func (s *Web) CreateEntity(entityType, parentId, name string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "create_entity",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
		Name:     name,
	})

	s.sendAndWait(msg)
}

func (s *Web) GetEntity(entityId string) data.Entity {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "get_entity",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntityRequest{
		Id: entityId,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebConfigGetEntityResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::GetEntity] Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigGetEntityResponse_SUCCESS {
		return nil
	}

	return entity.FromEntityPb(resp.Entity)
}

func (s *Web) SetEntity(e data.Entity) {
	// Set entity is handled through CreateEntity
}

func (s *Web) DeleteEntity(entityId string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "delete_entity",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigDeleteEntityRequest{
		Id: entityId,
	})

	s.sendAndWait(msg)
}

func (s *Web) FindEntities(entityType string) []string {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "get_entities",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeGetEntitiesRequest{
		EntityType: entityType,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebRuntimeGetEntitiesResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::FindEntities] Failed to unmarshal response: %v", err)
		return nil
	}

	ids := make([]string, len(resp.Entities))
	for i, e := range resp.Entities {
		ids[i] = e.Id
	}
	return ids
}

func (s *Web) GetEntityTypes() []string {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "get_entity_types",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntityTypesRequest{})

	response := s.sendAndWait(msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebConfigGetEntityTypesResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::GetEntityTypes] Failed to unmarshal response: %v", err)
		return nil
	}

	return resp.Types
}

func (s *Web) EntityExists(entityId string) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "entity_exists",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeEntityExistsRequest{
		EntityId: entityId,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return false
	}

	var resp protobufs.WebRuntimeEntityExistsResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::EntityExists] Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Exists
}

func (s *Web) FieldExists(fieldName, entityType string) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "field_exists",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return false
	}

	var resp protobufs.WebRuntimeFieldExistsResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::FieldExists] Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Exists
}

func (s *Web) GetEntitySchema(entityType string) data.EntitySchema {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "get_entity_schema",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntitySchemaRequest{
		Type: entityType,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebConfigGetEntitySchemaResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::GetEntitySchema] Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.WebConfigGetEntitySchemaResponse_SUCCESS {
		return nil
	}

	return entity.FromSchemaPb(resp.Schema)
}

func (s *Web) SetEntitySchema(schema data.EntitySchema) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "set_entity_schema",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigSetEntitySchemaRequest{
		Schema: entity.ToSchemaPb(schema),
	})

	s.sendAndWait(msg)
}

func (s *Web) Read(requests ...data.Request) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "read",
	}

	dbRequests := make([]*protobufs.DatabaseRequest, len(requests))
	for i, r := range requests {
		dbRequests[i] = request.ToPb(r)
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeDatabaseRequest{
		RequestType: protobufs.WebRuntimeDatabaseRequest_READ,
		Requests:    dbRequests,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return
	}

	var resp protobufs.WebRuntimeDatabaseResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::Read] Failed to unmarshal response: %v", err)
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

func (s *Web) Write(requests ...data.Request) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "write",
	}

	dbRequests := make([]*protobufs.DatabaseRequest, len(requests))
	for i, r := range requests {
		dbRequests[i] = request.ToPb(r)
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeDatabaseRequest{
		RequestType: protobufs.WebRuntimeDatabaseRequest_WRITE,
		Requests:    dbRequests,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return
	}

	var resp protobufs.WebRuntimeDatabaseResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::Write] Failed to unmarshal response: %v", err)
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
	if ch, ok := s.pendingResponses[msg.Header.Id]; ok {
		delete(s.pendingResponses, msg.Header.Id)
		s.mu.Unlock()
		ch <- msg
		close(ch)
		return
	}
	s.mu.Unlock()

	// Handle notifications
	if msg.Header.Id == "notification" {
		var notif protobufs.DatabaseNotification
		if err := msg.Payload.UnmarshalTo(&notif); err != nil {
			log.Error("[Web::handleMessage] Failed to unmarshal notification: %v", err)
			return
		}

		s.notificationsMutex.Lock()
		s.notifications = append(s.notifications, notification.FromPb(&notif))
		s.notificationsMutex.Unlock()
	}
}

func (s *Web) sendAndWait(msg web.Message) web.Message {
	if !s.IsConnected() {
		log.Error("[Web::sendAndWait] Not connected")
		return nil
	}

	responseCh := make(chan web.Message, 1)

	s.mu.Lock()
	s.pendingResponses[msg.Header.Id] = responseCh
	s.mu.Unlock()

	s.client.Write(msg)

	select {
	case response := <-responseCh:
		return response
	case <-time.After(10 * time.Second):
		log.Error("[Web::sendAndWait] Timeout waiting for response to %v", msg.Header.Id)
		s.mu.Lock()
		delete(s.pendingResponses, msg.Header.Id)
		s.mu.Unlock()
		return nil
	}
}

func (s *Web) Notify(config data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "register_notification",
	}

	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeRegisterNotificationRequest{
		Requests: []*protobufs.DatabaseNotificationConfig{notification.ToConfigPb(config)},
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return notification.NewToken("", s, nil)
	}

	var resp protobufs.WebRuntimeRegisterNotificationResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::Notify] Failed to unmarshal response: %v", err)
		return notification.NewToken("", s, nil)
	}

	if len(resp.Tokens) == 0 {
		return notification.NewToken("", s, nil)
	}

	token := resp.Tokens[0]
	s.callbacks[token] = append(s.callbacks[token], cb)

	return notification.NewToken(token, s, cb)
}

func (s *Web) Unnotify(token string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{
		Id: "unregister_notification",
	}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	})

	s.sendAndWait(msg)
	delete(s.callbacks, token)
}

func (s *Web) UnnotifyCallback(token string, cb data.NotificationCallback) {
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
		s.Unnotify(token)
	} else {
		s.callbacks[token] = callbacks
	}
}

func (s *Web) ProcessNotifications() {
	s.notificationsMutex.Lock()
	notifications := s.notifications
	s.notifications = nil
	s.notificationsMutex.Unlock()

	for _, n := range notifications {
		if callbacks, ok := s.callbacks[n.GetToken()]; ok {
			for _, cb := range callbacks {
				cb.Fn(n)
			}
		}
	}
}

// Implement remaining data.Store interface methods for temp storage and sorted sets
func (s *Web) TempSet(key, value string, expiration time.Duration) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "temp_set"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempSetRequest{
		Key:          key,
		Value:        value,
		ExpirationMs: expiration.Milliseconds(),
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return false
	}

	var resp protobufs.WebRuntimeTempSetResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::TempSet] Failed to unmarshal response: %v", err)
		return false
	}

	return resp.Success
}

func (s *Web) TempGet(key string) string {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "temp_get"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempGetRequest{
		Key: key,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return ""
	}

	var resp protobufs.WebRuntimeTempGetResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::TempGet] Failed to unmarshal response: %v", err)
		return ""
	}

	return resp.Value
}

func (s *Web) TempExpire(key string, expiration time.Duration) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "temp_expire"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempExpireRequest{
		Key:          key,
		ExpirationMs: expiration.Milliseconds(),
	})

	s.sendAndWait(msg)
}

func (s *Web) TempDel(key string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "temp_del"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeTempDelRequest{
		Key: key,
	})

	s.sendAndWait(msg)
}

func (s *Web) SortedSetAdd(key string, member string, score float64) int64 {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "sorted_set_add"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetAddRequest{
		Key:    key,
		Member: member,
		Score:  score,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return 0
	}

	var resp protobufs.WebRuntimeSortedSetAddResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::SortedSetAdd] Failed to unmarshal response: %v", err)
		return 0
	}

	return resp.Result
}

func (s *Web) SortedSetRemove(key string, member string) int64 {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "sorted_set_remove"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetRemoveRequest{
		Key:    key,
		Member: member,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return 0
	}

	var resp protobufs.WebRuntimeSortedSetRemoveResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::SortedSetRemove] Failed to unmarshal response: %v", err)
		return 0
	}

	return resp.Result
}

func (s *Web) SortedSetRemoveRangeByRank(key string, start, stop int64) int64 {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "sorted_set_remove_range_by_rank"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetRemoveRangeByRankRequest{
		Key:   key,
		Start: start,
		Stop:  stop,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return 0
	}

	var resp protobufs.WebRuntimeSortedSetRemoveRangeByRankResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::SortedSetRemoveRangeByRank] Failed to unmarshal response: %v", err)
		return 0
	}

	return resp.Result
}

func (s *Web) SortedSetRangeByScoreWithScores(key string, min, max string) []data.SortedSetMember {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{Id: "sorted_set_range_by_score_with_scores"}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeSortedSetRangeByScoreWithScoresRequest{
		Key: key,
		Min: min,
		Max: max,
	})

	response := s.sendAndWait(msg)
	if response == nil {
		return nil
	}

	var resp protobufs.WebRuntimeSortedSetRangeByScoreWithScoresResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("[Web::SortedSetRangeByScoreWithScores] Failed to unmarshal response: %v", err)
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

// ...implement remaining temp and sorted set methods following same pattern...
