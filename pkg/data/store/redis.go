package store

import (
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/data/transformer"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	MaxStreamLength = 50
)

type RedisConfig struct {
	Address  string
	Password string
}

// schema:entity:<type> -> DatabaseEntitySchema
// schema:field:<name> -> DatabaseFieldSchema
// instance:entity:<entityId> -> DatabaseEntity
// instance:field:<name>:<entityId> -> DatabaseField
// instance:type:<entityType> -> []string{entityId...}
// instance:notification-config:<entityId>:<fieldName> -> []string{subscriptionId...}
// instance:notification-config:<entityType>:<fieldName> -> []string{subscriptionId...}
type RedisKeyGenerator struct{}

func (g *RedisKeyGenerator) GetEntitySchemaKey(entityType string) string {
	return "schema:entity:" + entityType
}

func (g *RedisKeyGenerator) GetEntityKey(entityId string) string {
	return "instance:entity:" + entityId
}

func (g *RedisKeyGenerator) GetFieldKey(fieldName, entityId string) string {
	return "instance:field:" + fieldName + ":" + entityId
}

func (g *RedisKeyGenerator) GetEntityTypeKey(entityType string) string {
	return "instance:type:" + entityType
}

func (g *RedisKeyGenerator) GetEntityIdNotificationConfigKey(entityId, fieldName string) string {
	return "instance:notification-config:" + entityId + ":" + fieldName
}

func (g *RedisKeyGenerator) GetEntityTypeNotificationConfigKey(entityType, fieldName string) string {
	return "instance:notification-config:" + entityType + ":" + fieldName
}

func (g *RedisKeyGenerator) GetNotificationChannelKey(serviceId string) string {
	return "instance:notification:" + serviceId
}

type Redis struct {
	client              *redis.Client
	config              RedisConfig
	callbacks           map[string][]data.NotificationCallback
	lastStreamMessageId string
	keygen              RedisKeyGenerator
	transformer         data.Transformer
}

func NewRedis(config RedisConfig) data.Store {
	s := &Redis{
		config:              config,
		callbacks:           map[string][]data.NotificationCallback{},
		lastStreamMessageId: "$",
		keygen:              RedisKeyGenerator{},
	}

	s.transformer = transformer.NewTransformer(s)

	return s
}

func (s *Redis) Connect() {
	s.Disconnect()

	log.Info("[Redis::Connect] Connecting to %v", s.config.Address)
	s.client = redis.NewClient(&redis.Options{
		Addr:     s.config.Address,
		Password: s.config.Password,
	})
}

func (s *Redis) Disconnect() {
	if s.client == nil {
		return
	}

	s.client.Close()
	s.client = nil
}

func (s *Redis) IsConnected() bool {
	return s.client != nil && s.client.Ping(context.Background()).Err() == nil
}

func (s *Redis) CreateSnapshot() data.Snapshot {
	ss := snapshot.New()

	usedEntityType := map[string]bool{}
	usedFields := map[string]bool{}
	for _, entityType := range s.GetEntityTypes() {
		entitySchema := s.GetEntitySchema(entityType)
		for _, entityId := range s.FindEntities(entityType) {
			usedEntityType[entityType] = true
			ss.AppendEntity(s.GetEntity(entityId))
			for _, fieldName := range entitySchema.GetFieldNames() {
				r := request.New().SetEntityId(entityId).SetFieldName(fieldName)

				s.Read(r)

				if r.IsSuccessful() {
					ss.AppendField(field.FromRequest(r))
				}

				usedFields[fieldName] = true
			}
		}

		if usedEntityType[entityType] {
			ss.AppendSchema(entitySchema)
		}
	}

	return ss
}

func (s *Redis) RestoreSnapshot(ss data.Snapshot) {
	log.Info("[Redis::RestoreSnapshot] Restoring snapshot...")

	err := s.client.FlushDB(context.Background()).Err()
	if err != nil {
		log.Error("[Redis::RestoreSnapshot] Failed to flush database: %v", err)
		return
	}

	for _, sc := range ss.GetSchemas() {
		s.SetEntitySchema(sc)
		log.Debug("[Redis::RestoreSnapshot] Restored entity schema: %v", sc)
	}

	for _, e := range ss.GetEntities() {
		s.SetEntity(e)
		s.client.SAdd(context.Background(), s.keygen.GetEntityTypeKey(e.GetType()), e.GetId())
		log.Debug("[Redis::RestoreSnapshot] Restored entity: %v", e)
	}

	for _, f := range ss.GetFields() {
		s.Write(request.FromField(f))
		log.Debug("[Redis::RestoreSnapshot] Restored field: %v", f)
	}

	log.Info("[Redis::RestoreSnapshot] Snapshot restored.")
}

func (s *Redis) CreateEntity(entityType, parentId, name string) {
	entityId := uuid.New().String()

	sc := s.GetEntitySchema(entityType)
	if sc == nil {
		log.Error("[Redis::CreateEntity] Failed to get entity schema for type %s", entityType)
		return
	}

	// Initialize empty fields
	requests := []data.Request{}
	for _, fsc := range sc.GetFields() {
		requests = append(requests, request.New().SetEntityId(entityId).SetFieldName(fsc.GetFieldName()))
	}

	if len(requests) > 0 {
		s.Write(requests...)
	}

	p := &protobufs.DatabaseEntity{
		Id:       entityId,
		Name:     name,
		Parent:   &protobufs.EntityReference{Raw: parentId},
		Type:     entityType,
		Children: []*protobufs.EntityReference{},
	}
	b, err := proto.Marshal(p)
	if err != nil {
		log.Error("[Redis::CreateEntity] Failed to marshal entity: %v", err)
		return
	}

	s.client.SAdd(context.Background(), s.keygen.GetEntityTypeKey(entityType), entityId)
	s.client.Set(context.Background(), s.keygen.GetEntityKey(entityId), base64.StdEncoding.EncodeToString(b), 0)

	if parentId != "" {
		parent := s.GetEntity(parentId)
		if parent != nil {
			parent.AppendChildId(entityId)
			s.SetEntity(parent)
		} else {
			log.Error("[Redis::CreateEntity] Failed to get parent entity: %v", parentId)
		}
	}
}

func (s *Redis) GetEntity(entityId string) data.Entity {
	e, err := s.client.Get(context.Background(), s.keygen.GetEntityKey(entityId)).Result()
	if err != nil {
		log.Error("[Redis::GetEntity] Failed to get entity: %v", err)
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(e)
	if err != nil {
		log.Error("[Redis::GetEntity] Failed to decode entity: %v", err)
		return nil
	}

	p := &protobufs.DatabaseEntity{}
	err = proto.Unmarshal(b, p)
	if err != nil {
		log.Error("[Redis::GetEntity] Failed to unmarshal entity: %v", err)
		return nil
	}

	return entity.FromEntityPb(p)
}

func (s *Redis) SetEntity(e data.Entity) {
	b, err := proto.Marshal(entity.ToEntityPb(e))
	if err != nil {
		log.Error("[Redis::SetEntity] Failed to marshal entity: %v", err)
		return
	}

	err = s.client.Set(context.Background(), s.keygen.GetEntityKey(e.GetId()), base64.StdEncoding.EncodeToString(b), 0).Err()
	if err != nil {
		log.Error("[Redis::SetEntity] Failed to set entity '%s': %v", e.GetId(), err)
		return
	}
}

func (s *Redis) DeleteEntity(entityId string) {
	e := s.GetEntity(entityId)
	if e == nil {
		log.Error("[Redis::DeleteEntity] Failed to get entity: %v", entityId)
		return
	}

	parent := s.GetEntity(e.GetParentId())
	if parent != nil {
		parent.RemoveChildId(e.GetId())
		s.SetEntity(parent)
	}

	for _, c := range e.GetChildrenIds() {
		s.DeleteEntity(c)
	}

	for _, fieldName := range s.GetEntitySchema(e.GetType()).GetFieldNames() {
		s.client.Del(context.Background(), s.keygen.GetFieldKey(fieldName, entityId))
	}

	s.client.SRem(context.Background(), s.keygen.GetEntityTypeKey(e.GetType()), entityId)
	s.client.Del(context.Background(), s.keygen.GetEntityKey(entityId))
}

func (s *Redis) FindEntities(entityType string) []string {
	return s.client.SMembers(context.Background(), s.keygen.GetEntityTypeKey(entityType)).Val()
}

func (s *Redis) EntityExists(entityId string) bool {
	e, err := s.client.Get(context.Background(), s.keygen.GetEntityKey(entityId)).Result()
	if err != nil {
		return false
	}

	return e != ""
}

func (s *Redis) FieldExists(fieldName, entityType string) bool {
	if !strings.Contains(entityType, "-") {
		schema := s.GetEntitySchema(entityType)
		if schema != nil {
			f := schema.GetField(fieldName)
			return f != nil
		}
	}

	r := request.New()
	r.SetEntityId(entityType)
	r.SetFieldName(fieldName)

	s.Read(r)

	return r.IsSuccessful()
}

func (s *Redis) GetFieldSchema(entityType, fieldName string) data.FieldSchema {
	entitySchema := s.GetEntitySchema(entityType)
	if entitySchema == nil {
		log.Error("[Redis::GetFieldSchema] Failed to get entity schema for %s", entityType)
		return nil
	}

	f := entitySchema.GetField(fieldName)

	if f == nil {
		log.Error("[Redis::GetFieldSchema] Failed to find field schema: %s.%s", entityType, fieldName)
	}

	return f
}

func (s *Redis) SetFieldSchema(entityType, fieldName string, value data.FieldSchema) {
	entitySchema := s.GetEntitySchema(entityType)
	if entitySchema == nil {
		log.Error("[Redis::SetFieldSchema] Failed to get entity schema for %s", entityType)
		return
	}

	fields := entitySchema.GetFields()
	for i, f := range fields {
		if f.GetFieldName() == fieldName {
			fields[i] = value
			entitySchema.SetFields(fields)
			s.SetEntitySchema(entitySchema)
			return
		}
	}

	fields = append(fields, value)
	entitySchema.SetFields(fields)
	s.SetEntitySchema(entitySchema)
}

func (s *Redis) GetEntityTypes() []string {
	it := s.client.Scan(context.Background(), 0, s.keygen.GetEntitySchemaKey("*"), 0).Iterator()
	types := []string{}

	for it.Next(context.Background()) {
		types = append(types, strings.ReplaceAll(it.Val(), s.keygen.GetEntitySchemaKey(""), ""))
	}

	return types
}

func (s *Redis) GetEntitySchema(entityType string) data.EntitySchema {
	e, err := s.client.Get(context.Background(), s.keygen.GetEntitySchemaKey(entityType)).Result()
	if err != nil {
		log.Error("[Redis::GetEntitySchema] Failed to get entity schema (%v): %v", entityType, err)
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(e)
	if err != nil {
		log.Error("[Redis::GetEntitySchema] Failed to decode entity schema (%v): %v", entityType, err)
		return nil
	}

	p := &protobufs.DatabaseEntitySchema{}
	err = proto.Unmarshal(b, p)
	if err != nil {
		log.Error("[Redis::GetEntitySchema] Failed to unmarshal entity schema (%v): %v", entityType, err)
		return nil
	}

	return entity.FromSchemaPb(p)
}

func (s *Redis) SetEntitySchema(newSchema data.EntitySchema) {
	b, err := proto.Marshal(entity.ToSchemaPb(newSchema))
	if err != nil {
		log.Error("[Redis::SetEntitySchema] Failed to marshal entity schema: %v", err)
		return
	}

	oldSchema := s.GetEntitySchema(newSchema.GetType())
	if oldSchema != nil {
		removedFields := []string{}
		newFields := []string{}

		for _, fieldName := range oldSchema.GetFieldNames() {
			if newSchema.GetField(fieldName) == nil {
				removedFields = append(removedFields, fieldName)
			}
		}

		for _, fieldName := range newSchema.GetFieldNames() {
			if oldSchema.GetField(fieldName) == nil {
				newFields = append(newFields, fieldName)
			}
		}

		for _, entityId := range s.FindEntities(newSchema.GetType()) {
			for _, field := range removedFields {
				s.client.Del(context.Background(), s.keygen.GetFieldKey(field, entityId))
			}

			for _, field := range newFields {
				r := request.New().SetEntityId(entityId).SetFieldName(field)
				s.Write(r)
			}
		}
	}

	s.client.Set(context.Background(), s.keygen.GetEntitySchemaKey(newSchema.GetType()), base64.StdEncoding.EncodeToString(b), 0)
}

func (s *Redis) Read(requests ...data.Request) {
	for _, r := range requests {
		r.SetSuccessful(false)

		indirectField, indirectEntity := s.ResolveIndirection(r.GetFieldName(), r.GetEntityId())

		if indirectField == "" || indirectEntity == "" {
			log.Error("[Redis::Read] Failed to resolve indirection: %v", r)
			continue
		}

		e, err := s.client.Get(context.Background(), s.keygen.GetFieldKey(indirectField, indirectEntity)).Result()
		if err != nil {
			if err != redis.Nil {
				log.Error("[Redis::Read] Failed to read field: %v", err)
			} else {
				// If we can't read because the key doesn't exist, it's not a necessarily an issue.
				// It would be good to know from a troubleshooting aspect though.
				log.Trace("[Redis::Read] Failed to read field: %v", err)
			}
			continue
		}

		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[Redis::Read] Failed to decode field: %v", err)
			continue
		}

		p := &protobufs.DatabaseField{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[Redis::Read] Failed to unmarshal field: %v", err)
			continue
		}

		f := field.FromFieldPb(p)
		writeTime := f.GetWriteTime()
		writerId := f.GetWriter()

		r.SetValue(f.GetValue())
		r.SetWriteTime(&writeTime)
		r.SetWriter(&writerId)

		r.SetSuccessful(true)
	}
}

func (s *Redis) Write(requests ...data.Request) {
	for _, req := range requests {
		req.SetSuccessful(false)

		indirectField, indirectEntity := s.ResolveIndirection(req.GetFieldName(), req.GetEntityId())
		if indirectField == "" || indirectEntity == "" {
			log.Error("[Redis::Write] Failed to resolve indirection: %v", req)
			continue
		}

		e := s.GetEntity(indirectEntity)
		if e == nil {
			log.Error("[Redis::Write] Failed to get entity: %v", indirectEntity)
			continue
		}

		fs := s.GetFieldSchema(e.GetType(), indirectField)
		if fs == nil {
			log.Error("[Redis::Write] Failed to get field schema for %s", indirectField)
			continue
		}

		actualFieldType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(fs.GetFieldType()))
		if err != nil {
			log.Error("[Redis::Write] Failed to find message type %s: %v", fs.GetFieldType(), err)
			continue
		}

		if req.GetValue() == nil {
			a, err := anypb.New(actualFieldType.New().Interface())

			if err != nil {
				log.Error("[Redis::Write] Failed to create anypb for field %s.%s: %v", e.GetType(), fs.GetFieldName(), err)
				continue
			}

			v := field.FromAnyPb(a)
			req.SetValue(v)
		} else {
			a, err := anypb.New(actualFieldType.New().Interface())

			if err != nil {
				log.Error("[Redis::Write] Failed to create anypb for field %s.%s: %v", e.GetType(), fs.GetFieldName(), err)
				continue
			}

			v := field.FromAnyPb(a)

			if req.GetValue().GetType() != v.GetType() && !v.IsTransformation() {
				log.Warn("[Redis::Write] Field type mismatch for %s.%s. Got: %v, Expected: %v. Writing default value instead.", req.GetEntityId(), req.GetFieldName(), req.GetValue().GetType(), v.GetType())
				req.SetValue(v)
			}
		}

		if req.GetWriteTime() == nil {
			wt := time.Now()
			req.SetWriteTime(&wt)
		}

		if req.GetWriter() == nil {
			wr := ""
			req.SetWriter(&wr)
		}

		oldReq := request.New().SetEntityId(req.GetEntityId()).SetFieldName(req.GetFieldName())
		s.Read(oldReq)

		// Set the value in the database
		// Note that for a transformation, we don't actually write the value to the database
		// unless the new value is a transformation. This is because the transformation is
		// executed by the transformer, which will write the result to the database.
		if oldReq.IsSuccessful() && oldReq.GetValue().IsTransformation() && !req.GetValue().IsTransformation() {
			src := oldReq.GetValue().GetTransformation()
			s.transformer.Transform(src, req)
			req.SetValue(oldReq.GetValue())
		}

		p := field.ToFieldPb(field.FromRequest(req))

		b, err := proto.Marshal(p)
		if err != nil {
			log.Error("[Redis::Write] Failed to marshal field: %v", err)
			continue
		}

		p.Id = indirectEntity
		p.Name = indirectField

		_, err = s.client.Set(context.Background(), s.keygen.GetFieldKey(indirectField, indirectEntity), base64.StdEncoding.EncodeToString(b), 0).Result()

		// Notify listeners of the change
		s.triggerNotifications(req, oldReq)

		if err != nil {
			log.Error("[Redis::Write] Failed to write field: %v", err)
			continue
		}
		req.SetSuccessful(true)
	}
}

func (s *Redis) Notify(nc data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	if nc.GetServiceId() == "" {
		nc.SetServiceId(s.getServiceId())
	}

	b, err := proto.Marshal(notification.ToConfigPb(nc))
	if err != nil {
		log.Error("[Redis::Notify] Failed to marshal notification config: %v", err)
		return notification.NewToken("", s, nil)
	}

	e := base64.StdEncoding.EncodeToString(b)

	if s.lastStreamMessageId == "$" {
		r, err := s.client.XInfoStream(context.Background(), s.keygen.GetNotificationChannelKey(s.getServiceId())).Result()
		if err != nil {
			s.lastStreamMessageId = "0"
		} else {
			s.lastStreamMessageId = r.LastGeneratedID
		}
	}

	if nc.GetEntityId() != "" && s.FieldExists(nc.GetFieldName(), nc.GetEntityId()) {
		s.client.SAdd(context.Background(), s.keygen.GetEntityIdNotificationConfigKey(nc.GetEntityId(), nc.GetFieldName()), e)
		s.callbacks[e] = append(s.callbacks[e], cb)
		return notification.NewToken(e, s, cb)
	}

	if nc.GetEntityType() != "" && s.FieldExists(nc.GetFieldName(), nc.GetEntityType()) {
		s.client.SAdd(context.Background(), s.keygen.GetEntityTypeNotificationConfigKey(nc.GetEntityType(), nc.GetFieldName()), e)
		s.callbacks[e] = append(s.callbacks[e], cb)
		return notification.NewToken(e, s, cb)
	}

	log.Error("[Redis::Notify] Failed to find field: %v", nc)
	return notification.NewToken("", s, nil)
}

func (s *Redis) Unnotify(e string) {
	if s.callbacks[e] == nil {
		log.Error("[Redis::Unnotify] Failed to find callback: %v", e)
		return
	}

	delete(s.callbacks, e)
}

func (s *Redis) UnnotifyCallback(e string, c data.NotificationCallback) {
	if s.callbacks[e] == nil {
		log.Warn("[Redis::UnnotifyCallback] Failed to find callback: %v", e)
		return
	}

	callbacks := []data.NotificationCallback{}
	for _, callback := range s.callbacks[e] {
		if callback.Id() != c.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	s.callbacks[e] = callbacks
}

func (s *Redis) ProcessNotifications() {
	s.transformer.ProcessPending()

	r, err := s.client.XRead(context.Background(), &redis.XReadArgs{
		Streams: []string{s.keygen.GetNotificationChannelKey(s.getServiceId()), s.lastStreamMessageId},
		Count:   1000,
		Block:   -1,
	}).Result()

	if err != nil && err != redis.Nil {
		log.Error("[Redis::ProcessNotifications] Failed to read stream %v: %v", s.keygen.GetNotificationChannelKey(s.getServiceId()), err)
		return
	}

	for _, x := range r {
		for _, m := range x.Messages {
			s.lastStreamMessageId = m.ID
			decodedMessage := make(map[string]string)

			for key, value := range m.Values {
				if castedValue, ok := value.(string); ok {
					decodedMessage[key] = castedValue
				} else {
					log.Error("[Redis::ProcessNotifications] Failed to cast value: %v", value)
					continue
				}
			}

			if data, ok := decodedMessage["data"]; ok {
				p, err := base64.StdEncoding.DecodeString(data)
				if err != nil {
					log.Error("[Redis::ProcessNotifications] Failed to decode notification: %v", err)
					continue
				}

				n := &protobufs.DatabaseNotification{}
				err = proto.Unmarshal(p, n)
				if err != nil {
					log.Error("[Redis::ProcessNotifications] Failed to unmarshal notification: %v", err)
					continue
				}

				for _, callback := range s.callbacks[n.Token] {
					callback.Fn(notification.FromPb(n))
				}
			}
		}
	}
}

func (s *Redis) ResolveIndirection(indirectField, entityId string) (string, string) {
	fields := strings.Split(indirectField, "->")

	if len(fields) == 1 {
		return indirectField, entityId
	}

	for _, f := range fields[:len(fields)-1] {
		r := request.New().SetEntityId(entityId).SetFieldName(f)

		s.Read(r)

		if r.IsSuccessful() {
			v := r.GetValue()
			if v.IsEntityReference() {
				entityId = v.GetEntityReference()

				if entityId == "" {
					log.Error("[Redis::ResolveIndirection] Failed to resolve entity reference: %v", r)
					return "", ""
				}

				continue
			}

			log.Error("[Redis::ResolveIndirection] Field is not an entity reference: %v", r)
			return "", ""
		}

		// Fallback to parent entity reference by name
		entity := s.GetEntity(entityId)
		if entity == nil {
			log.Error("[Redis::ResolveIndirection] Failed to get entity: %v", entityId)
			return "", ""
		}

		parentId := entity.GetParentId()
		if parentId != "" {
			parentEntity := s.GetEntity(parentId)

			if parentEntity != nil && parentEntity.GetName() == f {
				entityId = parentId
				continue
			}
		}

		// Fallback to child entity reference by name
		foundChild := false
		for _, childId := range entity.GetChildrenIds() {
			childEntity := s.GetEntity(childId)
			if childEntity == nil {
				log.Error("[Redis::ResolveIndirection] Failed to get child entity: %v", childId)
				continue
			}

			if childEntity.GetName() == f {
				entityId = childId
				foundChild = true
				break
			}
		}

		if !foundChild {
			log.Error("[Redis::ResolveIndirection] Failed to find child entity: %v", f)
			return "", ""
		}
	}

	return fields[len(fields)-1], entityId
}

func (s *Redis) triggerNotifications(r data.Request, o data.Request) {
	// failed to read old value (it may not exist initially)
	if !o.IsSuccessful() {
		log.Warn("[Redis::triggerNotifications] Failed to read old value: %v", o)
		return
	}

	changed := !proto.Equal(field.ToAnyPb(r.GetValue()), field.ToAnyPb(o.GetValue()))

	indirectField, indirectEntity := s.ResolveIndirection(r.GetFieldName(), r.GetEntityId())

	if indirectField == "" || indirectEntity == "" {
		log.Error("[Redis::triggerNotifications] Failed to resolve indirection: %v", r)
		return
	}

	m, err := s.client.SMembers(context.Background(), s.keygen.GetEntityIdNotificationConfigKey(indirectEntity, indirectField)).Result()
	if err != nil {
		log.Error("[Redis::triggerNotifications] Failed to get notification config: %v", err)
		return
	}

	for _, e := range m {
		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to decode notification config: %v", err)
			continue
		}

		p := &protobufs.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to unmarshal notification config: %v", err)
			continue
		}
		nc := notification.FromConfigPb(p)

		if nc.GetNotifyOnChange() && !changed {
			continue
		}

		n := &protobufs.DatabaseNotification{
			Token:    e,
			Current:  field.ToFieldPb(field.FromRequest(r)),
			Previous: field.ToFieldPb(field.FromRequest(o)),
			Context:  []*protobufs.DatabaseField{},
		}

		for _, cf := range nc.GetContextFields() {
			cr := request.New().SetEntityId(indirectEntity).SetFieldName(cf)
			s.Read(cr)
			if cr.IsSuccessful() {
				n.Context = append(n.Context, field.ToFieldPb(field.FromRequest(cr)))
			}
		}

		b, err = proto.Marshal(n)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to marshal notification: %v", err)
			continue
		}

		_, err = s.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: s.keygen.GetNotificationChannelKey(p.ServiceId),
			Values: []string{"data", base64.StdEncoding.EncodeToString(b)},
			MaxLen: MaxStreamLength,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to add notification: %v", err)
			continue
		}
	}

	fetchedEntity := s.GetEntity(indirectEntity)
	if fetchedEntity == nil {
		log.Error("[Redis::triggerNotifications] Failed to get entity: %v (indirect=%v)", r.GetEntityId(), indirectEntity)
		return
	}

	m, err = s.client.SMembers(context.Background(), s.keygen.GetEntityTypeNotificationConfigKey(fetchedEntity.GetType(), indirectField)).Result()
	if err != nil {
		log.Error("[Redis::triggerNotifications] Failed to get notification config: %v", err)
		return
	}

	for _, e := range m {
		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to decode notification config: %v", err)
			continue
		}

		p := &protobufs.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to unmarshal notification config: %v", err)
			continue
		}

		nc := notification.FromConfigPb(p)
		if nc.GetNotifyOnChange() && !changed {
			continue
		}

		n := &protobufs.DatabaseNotification{
			Token:    e,
			Current:  field.ToFieldPb(field.FromRequest(r)),
			Previous: field.ToFieldPb(field.FromRequest(o)),
			Context:  []*protobufs.DatabaseField{},
		}

		for _, cf := range nc.GetContextFields() {
			cr := request.New().SetEntityId(indirectEntity).SetFieldName(cf)
			s.Read(cr)
			if cr.IsSuccessful() {
				n.Context = append(n.Context, field.ToFieldPb(field.FromRequest(cr)))
			}
		}

		b, err = proto.Marshal(n)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to marshal notification: %v", err)
			continue
		}

		_, err = s.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: s.keygen.GetNotificationChannelKey(p.ServiceId),
			Values: []string{"data", base64.StdEncoding.EncodeToString(b)},
			MaxLen: MaxStreamLength,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to add notification: %v", err)
			continue
		}
	}
}

func (s *Redis) TempSet(key, value string, expiration time.Duration) bool {
	r, err := s.client.SetNX(context.Background(), key, value, expiration).Result()
	if err != nil {
		return false
	}

	return r
}

func (s *Redis) TempGet(key string) string {
	r, err := s.client.Get(context.Background(), key).Result()
	if err != nil {
		return ""
	}

	return r
}

func (s *Redis) TempExpire(key string, expiration time.Duration) {
	s.client.Expire(context.Background(), key, expiration)
}

func (s *Redis) TempDel(key string) {
	s.client.Del(context.Background(), key)
}

func (s *Redis) SortedSetAdd(key string, member string, score float64) int64 {
	result, err := s.client.ZAdd(context.Background(), key, redis.Z{
		Score:  score,
		Member: member,
	}).Result()
	if err != nil {
		log.Error("[Redis::SortedSetAdd] Failed to add member to sorted set: %v", err)
		return 0
	}
	return result
}

func (s *Redis) SortedSetRemove(key string, member string) int64 {
	result, err := s.client.ZRem(context.Background(), key, member).Result()
	if err != nil {
		log.Error("[Redis::SortedSetRemove] Failed to remove member from sorted set: %v", err)
		return 0
	}
	return result
}

func (s *Redis) SortedSetRemoveRangeByRank(key string, start, stop int64) int64 {
	result, err := s.client.ZRemRangeByRank(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Error("[Redis::SortedSetRemoveRangeByRank] Failed to remove range from sorted set: %v", err)
		return 0
	}
	return result
}

func (s *Redis) SortedSetRangeByScoreWithScores(key string, min, max string) []data.SortedSetMember {
	result, err := s.client.ZRangeByScoreWithScores(context.Background(), key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		log.Error("[Redis::SortedSetRangeByScoreWithScores] Failed to get range from sorted set: %v", err)
		return nil
	}
	members := make([]data.SortedSetMember, len(result))
	for i, z := range result {
		members[i] = data.SortedSetMember{
			Score:  z.Score,
			Member: z.Member.(string),
		}
	}
	return members
}

func (s *Redis) getServiceId() string {
	return app.GetApplicationName()
}
