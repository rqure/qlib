package store

import (
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
)

type RedisConfig struct {
	Address   string
	Password  string
	ServiceID func() string
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
	getServiceId        func() string
	transformer         ITransformer // Transformer calls scripts to transform field values of type Transformation
}

func NewRedis(config RedisConfig) data.Store {
	getServiceId := config.ServiceID
	if config.ServiceID == nil {
		getServiceId = GetApplicationName
	}

	s := &Redis{
		config:              config,
		callbacks:           map[string][]data.NotificationCallback{},
		lastStreamMessageId: "$",
		keygen:              RedisKeyGenerator{},
		getServiceId:        getServiceId,
	}

	s.transformer = NewTransformer(s)

	return s
}

func (s *Redis) Connect() {
	s.Disconnect()

	log.Info("[Redis::Connect] Connecting to %v", s.config.Address)
	s.client = redis.NewClient(&redis.Options{
		Addr:     s.config.Address,
		Password: s.config.Password,
		s:        0,
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

	err := s.client.Flushs(context.Background()).Err()
	if err != nil {
		log.Error("[Redis::RestoreSnapshot] Failed to flush database: %v", err)
		return
	}

	for _, schema := range ss.GetSchemas() {
		s.SetEntitySchema(schema.Name, schema)
		log.Debug("[Redis::RestoreSnapshot] Restored entity schema: %v", schema)
	}

	for _, schema := range ss.FieldSchemas {
		s.SetFieldSchema(schema.Name, schema)
		log.Debug("[Redis::RestoreSnapshot] Restored field schema: %v", schema)
	}

	for _, entity := range ss.Entities {
		s.SetEntity(entity.Id, entity)
		s.client.SAdd(context.Background(), s.keygen.GetEntityTypeKey(entity.Type), entity.Id)
		log.Debug("[Redis::RestoreSnapshot] Restored entity: %v", entity)
	}

	for _, field := range ss.Fields {
		s.Write([]*pb.DatabaseRequest{
			{
				Id:        field.Id,
				Field:     field.Name,
				Value:     field.Value,
				WriteTime: &pb.Timestamp{Raw: field.WriteTime},
				WriterId:  &pb.String{Raw: field.WriterId},
			},
		})
		log.Debug("[Redis::RestoreSnapshot] Restored field: %v", field)
	}

	log.Info("[Redis::RestoreSnapshot] Snapshot restored.")
}

func (s *Redis) CreateEntity(entityType, parentId, name string) {
	entityId := uuid.New().String()

	schema := s.GetEntitySchema(entityType)
	if schema == nil {
		log.Error("[Redis::CreateEntity] Failed to get entity schema for type %s", entityType)
		return
	}

	// Initialize empty fields
	requests := []*pb.DatabaseRequest{}
	for _, fieldName := range schema.Fields {
		fieldSchema := s.GetFieldSchema(fieldName)
		if fieldSchema == nil {
			log.Error("[Redis::CreateEntity] Failed to get field schema for %s", fieldName)
			continue
		}

		requests = append(requests, &pb.DatabaseRequest{
			Id:    entityId,
			Field: fieldName,
		})
	}

	if len(requests) > 0 {
		s.Write(requests)
	}

	p := &pb.DatabaseEntity{
		Id:       entityId,
		Name:     name,
		Parent:   &pb.EntityReference{Raw: parentId},
		Type:     entityType,
		Children: []*pb.EntityReference{},
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
			parent.Children = append(parent.Children, &pb.EntityReference{Raw: entityId})
			s.SetEntity(parentId, parent)
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

func (s *Redis) SetEntity(entityId string, value *pb.DatabaseEntity) {
	b, err := proto.Marshal(value)
	if err != nil {
		log.Error("[Redis::SetEntity] Failed to marshal entity: %v", err)
		return
	}

	err = s.client.Set(context.Background(), s.keygen.GetEntityKey(entityId), base64.StdEncoding.EncodeToString(b), 0).Err()
	if err != nil {
		log.Error("[Redis::SetEntity] Failed to set entity '%s': %v", entityId, err)
		return
	}
}

func (s *Redis) DeleteEntity(entityId string) {
	p := s.GetEntity(entityId)
	if p == nil {
		log.Error("[Redis::DeleteEntity] Failed to get entity: %v", entityId)
		return
	}

	parent := s.GetEntity(p.Parent.Raw)
	if parent != nil {
		newChildren := []*pb.EntityReference{}
		for _, child := range parent.Children {
			if child.Raw != entityId {
				newChildren = append(newChildren, child)
			}
		}
		parent.Children = newChildren
		s.SetEntity(p.Parent.Raw, parent)
	}

	for _, child := range p.Children {
		s.DeleteEntity(child.Raw)
	}

	for _, fieldName := range s.GetEntitySchema(p.Type).Fields {
		s.client.Del(context.Background(), s.keygen.GetFieldKey(fieldName, entityId))
	}

	s.client.SRem(context.Background(), s.keygen.GetEntityTypeKey(p.Type), entityId)
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
			s.SetEntitySchema(entityType, entitySchema)
			return
		}
	}

	fields = append(fields, value)
	entitySchema.SetFields(fields)
	s.SetEntitySchema(entityType, entitySchema)
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
	for _, r := range requests {
		r.SetSuccessful(false)

		indirectField, indirectEntity := s.ResolveIndirection(r.GetFieldName(), r.GetEntityId())
		if indirectField == "" || indirectEntity == "" {
			log.Error("[Redis::Write] Failed to resolve indirection: %v", r)
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

		if r.GetValue() == nil {
			a, err := anypb.New(actualFieldType.New().Interface())

			if err != nil {
				log.Error("[Redis::Write] Failed to create anypb for field %s.%s: %v", e.GetType(), fs.GetFieldName(), err)
				continue
			}

			v := field.FromAnyPb(a)
			r.SetValue(v)
		} else {
			a, err := anypb.New(actualFieldType.New().Interface())

			if err != nil {
				log.Error("[Redis::Write] Failed to create anypb for field %s.%s: %v", e.GetType(), fs.GetFieldName(), err)
				continue
			}

			v := field.FromAnyPb(a)

			if r.GetValue().GetType() != v.GetType() && !v.IsTransformation() {
				log.Warn("[Redis::Write] Field type mismatch for %s.%s. Got: %v, Expected: %v. Writing default value instead.", r.GetEntityId(), r.GetFieldName(), r.GetValue().GetType(), v.GetType())
				r.SetValue(v)
			}
		}

		if r.GetWriteTime() == nil {
			wt := time.Now()
			r.SetWriteTime(&wt)
		}

		if r.GetWriter() == nil {
			wr := ""
			r.SetWriter(&wr)
		}

		oldRequest := request.New().SetEntityId(r.GetEntityId()).SetFieldName(r.GetFieldName())
		s.Read(oldRequest)

		// Set the value in the database
		// Note that for a transformation, we don't actually write the value to the database
		// unless the new value is a transformation. This is because the transformation is
		// executed by the transformer, which will write the result to the database.
		if oldRequest.IsSuccessful() && oldRequest.GetValue().IsTransformation() && !r.GetValue().IsTransformation() {
			transformation := oldRequest.GetValue().GetTransformation()
			field := NewField(s, r.Id, r.Field)
			field.req = &pb.DatabaseRequest{
				Id:      r.Id,
				Field:   r.Field,
				Value:   r.Value,
				Success: true,
			}
			s.transformer.Transform(transformation, field)
			r.SetValue(oldRequest.GetValue())
		}

		p := field.ToFieldPb(field.FromRequest(r))

		b, err := proto.Marshal(p)
		if err != nil {
			log.Error("[Redis::Write] Failed to marshal field: %v", err)
			continue
		}

		p.Id = indirectEntity
		p.Name = indirectField

		_, err = s.client.Set(context.Background(), s.keygen.GetFieldKey(indirectField, indirectEntity), base64.StdEncoding.EncodeToString(b), 0).Result()

		// Notify listeners of the change
		s.triggerNotifications(r, oldRequest)

		if err != nil {
			log.Error("[Redis::Write] Failed to write field: %v", err)
			continue
		}
		r.SetSuccessful(true)
	}
}

func (s *Redis) Notify(nc data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	if nc.GetServiceId() == "" {
		nc.SetServiceId(s.getServiceId())
	}

	b, err := proto.Marshal(notification.ToConfigPb(nc))
	if err != nil {
		log.Error("[Redis::Notify] Failed to marshal notification config: %v", err)
		return &NotificationToken{
			s:              s,
			subscriptionId: "",
			callback:       nil,
		}
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

	if nc.Id != "" && s.FieldExists(nc.Field, nc.Id) {
		s.client.SAdd(context.Background(), s.keygen.GetEntityIdNotificationConfigKey(nc.Id, nc.Field), e)
		s.callbacks[e] = append(s.callbacks[e], cb)
		return &NotificationToken{
			s:              s,
			subscriptionId: e,
			callback:       cb,
		}
	}

	if nc.Type != "" && s.FieldExists(nc.Field, nc.Type) {
		s.client.SAdd(context.Background(), s.keygen.GetEntityTypeNotificationConfigKey(nc.Type, nc.Field), e)
		s.callbacks[e] = append(s.callbacks[e], cb)
		return &NotificationToken{
			s:              s,
			subscriptionId: e,
			callback:       cb,
		}
	}

	log.Warn("[Redis::Notify] Failed to find field: %v", nc)
	return &NotificationToken{
		s:              s,
		subscriptionId: "",
		callback:       nil,
	}
}

func (s *Redis) Unnotify(e string) {
	if s.callbacks[e] == nil {
		log.Warn("[Redis::Unnotify] Failed to find callback: %v", e)
		return
	}

	delete(s.callbacks, e)
}

func (s *Redis) UnnotifyCallback(e string, c NotificationCallback) {
	if s.callbacks[e] == nil {
		log.Warn("[Redis::UnnotifyCallback] Failed to find callback: %v", e)
		return
	}

	callbacks := []NotificationCallback{}
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

				n := &pb.DatabaseNotification{}
				err = proto.Unmarshal(p, n)
				if err != nil {
					log.Error("[Redis::ProcessNotifications] Failed to unmarshal notification: %v", err)
					continue
				}

				for _, callback := range s.callbacks[n.Token] {
					callback.Fn(n)
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

	for _, field := range fields[:len(fields)-1] {
		request := &pb.DatabaseRequest{
			Id:    entityId,
			Field: field,
		}

		s.Read([]*pb.DatabaseRequest{request})

		if request.Success {
			entityReference := &pb.EntityReference{}
			if request.Value.MessageIs(entityReference) {
				err := request.Value.UnmarshalTo(entityReference)
				if err != nil {
					log.Error("[Redis::ResolveIndirection] Failed to unmarshal entity reference: %v", err)
					return "", ""
				}

				entityId = entityReference.Raw
				continue
			}

			log.Error("[Redis::ResolveIndirection] Field is not an entity reference: %v", request)
			return "", ""
		}

		// Fallback to parent entity reference by name
		entity := s.GetEntity(entityId)
		if entity == nil {
			log.Error("[Redis::ResolveIndirection] Failed to get entity: %v", entityId)
			return "", ""
		}

		if entity.Parent != nil && entity.Parent.Raw != "" {
			parentEntity := s.GetEntity(entity.Parent.Raw)

			if parentEntity != nil && parentEntity.Name == field {
				entityId = entity.Parent.Raw
				continue
			}
		}

		// Fallback to child entity reference by name
		foundChild := false
		for _, child := range entity.Children {
			childEntity := s.GetEntity(child.Raw)
			if childEntity == nil {
				log.Error("[Redis::ResolveIndirection] Failed to get child entity: %v", child.Raw)
				continue
			}

			if childEntity.Name == field {
				entityId = child.Raw
				foundChild = true
				break
			}
		}

		if !foundChild {
			log.Error("[Redis::ResolveIndirection] Failed to find child entity: %v", field)
			return "", ""
		}
	}

	return fields[len(fields)-1], entityId
}

func (s *Redis) triggerNotifications(request *pb.DatabaseRequest, oldRequest *pb.DatabaseRequest) {
	// failed to read old value (it may not exist initially)
	if !oldRequest.Success {
		log.Warn("[Redis::triggerNotifications] Failed to read old value: %v", oldRequest)
		return
	}

	changed := !proto.Equal(request.Value, oldRequest.Value)

	indirectField, indirectEntity := s.ResolveIndirection(request.Field, request.Id)

	if indirectField == "" || indirectEntity == "" {
		log.Error("[Redis::triggerNotifications] Failed to resolve indirection: %v", request)
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

		p := &pb.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to unmarshal notification config: %v", err)
			continue
		}

		if p.NotifyOnChange && !changed {
			continue
		}

		n := &pb.DatabaseNotification{
			Token:    e,
			Current:  new(pb.DatabaseField).FromRequest(request),
			Previous: new(pb.DatabaseField).FromRequest(oldRequest),
			Context:  []*pb.DatabaseField{},
		}

		for _, context := range p.ContextFields {
			contextRequest := &pb.DatabaseRequest{
				Id:    indirectEntity,
				Field: context,
			}
			s.Read([]*pb.DatabaseRequest{contextRequest})
			if contextRequest.Success {
				n.Context = append(n.Context, new(pb.DatabaseField).FromRequest(contextRequest))
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
			MaxLen: 1000,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to add notification: %v", err)
			continue
		}
	}

	entity := s.GetEntity(indirectEntity)
	if entity == nil {
		log.Error("[Redis::triggerNotifications] Failed to get entity: %v (indirect=%v)", request.Id, indirectEntity)
		return
	}

	m, err = s.client.SMembers(context.Background(), s.keygen.GetEntityTypeNotificationConfigKey(entity.Type, indirectField)).Result()
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

		p := &pb.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[Redis::triggerNotifications] Failed to unmarshal notification config: %v", err)
			continue
		}

		if p.NotifyOnChange && !changed {
			continue
		}

		n := &pb.DatabaseNotification{
			Token:    e,
			Current:  new(pb.DatabaseField).FromRequest(request),
			Previous: new(pb.DatabaseField).FromRequest(oldRequest),
			Context:  []*pb.DatabaseField{},
		}

		for _, context := range p.ContextFields {
			contextRequest := &pb.DatabaseRequest{
				Id:    indirectEntity,
				Field: context,
			}
			s.Read([]*pb.DatabaseRequest{contextRequest})
			if contextRequest.Success {
				n.Context = append(n.Context, new(pb.DatabaseField).FromRequest(contextRequest))
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
			MaxLen: 100,
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

func (s *Redis) SortedSetRangeByScoreWithScores(key string, min, max string) []SortedSetMember {
	result, err := s.client.ZRangeByScoreWithScores(context.Background(), key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		log.Error("[Redis::SortedSetRangeByScoreWithScores] Failed to get range from sorted set: %v", err)
		return nil
	}
	members := make([]SortedSetMember, len(result))
	for i, z := range result {
		members[i] = SortedSetMember{
			Score:  z.Score,
			Member: z.Member.(string),
		}
	}
	return members
}
