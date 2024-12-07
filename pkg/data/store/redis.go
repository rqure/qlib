package store

import (
	"context"
	"encoding/base64"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RedisStoreConfig struct {
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
type RedisStoreKeyGenerator struct{}

func (g *RedisStoreKeyGenerator) GetEntitySchemaKey(entityType string) string {
	return "schema:entity:" + entityType
}

func (g *RedisStoreKeyGenerator) GetFieldSchemaKey(entityType, fieldName string) string {
	return "schema:field:" + entityType + ":" + fieldName
}

func (g *RedisStoreKeyGenerator) GetEntityKey(entityId string) string {
	return "instance:entity:" + entityId
}

func (g *RedisStoreKeyGenerator) GetFieldKey(fieldName, entityId string) string {
	return "instance:field:" + fieldName + ":" + entityId
}

func (g *RedisStoreKeyGenerator) GetEntityTypeKey(entityType string) string {
	return "instance:type:" + entityType
}

func (g *RedisStoreKeyGenerator) GetEntityIdNotificationConfigKey(entityId, fieldName string) string {
	return "instance:notification-config:" + entityId + ":" + fieldName
}

func (g *RedisStoreKeyGenerator) GetEntityTypeNotificationConfigKey(entityType, fieldName string) string {
	return "instance:notification-config:" + entityType + ":" + fieldName
}

func (g *RedisStoreKeyGenerator) GetNotificationChannelKey(serviceId string) string {
	return "instance:notification:" + serviceId
}

type RedisStore struct {
	client              *redis.Client
	config              RedisStoreConfig
	callbacks           map[string][]NotificationCallback
	lastStreamMessageId string
	keygen              RedisStoreKeyGenerator
	getServiceId        func() string
	transformer         ITransformer // Transformer calls scripts to transform field values of type Transformation
}

func NewRedisStore(config RedisStoreConfig) data.Store {
	getServiceId := config.ServiceID
	if config.ServiceID == nil {
		getServiceId = GetApplicationName
	}

	db := &RedisStore{
		config:              config,
		callbacks:           map[string][]NotificationCallback{},
		lastStreamMessageId: "$",
		keygen:              RedisStoreKeyGenerator{},
		getServiceId:        getServiceId,
	}

	db.transformer = NewTransformer(db)

	return db
}

func (db *RedisStore) Connect() {
	db.Disconnect()

	log.Info("[RedisStore::Connect] Connecting to %v", db.config.Address)
	db.client = redis.NewClient(&redis.Options{
		Addr:     db.config.Address,
		Password: db.config.Password,
		DB:       0,
	})
}

func (db *RedisStore) Disconnect() {
	if db.client == nil {
		return
	}

	db.client.Close()
	db.client = nil
}

func (db *RedisStore) IsConnected() bool {
	return db.client != nil && db.client.Ping(context.Background()).Err() == nil
}

func (db *RedisStore) CreateSnapshot() data.Snapshot {
	snapshot := &pb.DatabaseSnapshot{}

	usedEntityType := map[string]bool{}
	usedFields := map[string]bool{}
	for _, entityType := range db.GetEntityTypes() {
		entitySchema := db.GetEntitySchema(entityType)
		for _, entityId := range db.FindEntities(entityType) {
			usedEntityType[entityType] = true
			snapshot.Entities = append(snapshot.Entities, db.GetEntity(entityId))
			for _, fieldName := range entitySchema.Fields {
				request := &pb.DatabaseRequest{
					Id:    entityId,
					Field: fieldName,
				}
				db.Read([]*pb.DatabaseRequest{request})
				if request.Success {
					snapshot.Fields = append(snapshot.Fields, new(pb.DatabaseField).FromRequest(request))
				}
				usedFields[fieldName] = true
			}
		}

		if usedEntityType[entityType] {
			snapshot.EntitySchemas = append(snapshot.EntitySchemas, entitySchema)
		}
	}

	for _, fieldSchema := range db.GetFieldSchemas() {
		if usedFields[fieldSchema.Name] {
			snapshot.FieldSchemas = append(snapshot.FieldSchemas, fieldSchema)
		}
	}

	return snapshot
}

func (db *RedisStore) RestoreSnapshot(snapshot *pb.DatabaseSnapshot) {
	log.Info("[RedisStore::RestoreSnapshot] Restoring snapshot...")

	err := db.client.FlushDB(context.Background()).Err()
	if err != nil {
		log.Error("[RedisStore::RestoreSnapshot] Failed to flush database: %v", err)
		return
	}

	for _, schema := range snapshot.EntitySchemas {
		db.SetEntitySchema(schema.Name, schema)
		log.Debug("[RedisStore::RestoreSnapshot] Restored entity schema: %v", schema)
	}

	for _, schema := range snapshot.FieldSchemas {
		db.SetFieldSchema(schema.Name, schema)
		log.Debug("[RedisStore::RestoreSnapshot] Restored field schema: %v", schema)
	}

	for _, entity := range snapshot.Entities {
		db.SetEntity(entity.Id, entity)
		db.client.SAdd(context.Background(), db.keygen.GetEntityTypeKey(entity.Type), entity.Id)
		log.Debug("[RedisStore::RestoreSnapshot] Restored entity: %v", entity)
	}

	for _, field := range snapshot.Fields {
		db.Write([]*pb.DatabaseRequest{
			{
				Id:        field.Id,
				Field:     field.Name,
				Value:     field.Value,
				WriteTime: &pb.Timestamp{Raw: field.WriteTime},
				WriterId:  &pb.String{Raw: field.WriterId},
			},
		})
		log.Debug("[RedisStore::RestoreSnapshot] Restored field: %v", field)
	}

	log.Info("[RedisStore::RestoreSnapshot] Snapshot restored.")
}

func (db *RedisStore) CreateEntity(entityType, parentId, name string) {
	entityId := uuid.New().String()

	schema := db.GetEntitySchema(entityType)
	if schema == nil {
		log.Error("[RedisStore::CreateEntity] Failed to get entity schema for type %s", entityType)
		return
	}

	// Initialize empty fields
	requests := []*pb.DatabaseRequest{}
	for _, fieldName := range schema.Fields {
		fieldSchema := db.GetFieldSchema(fieldName)
		if fieldSchema == nil {
			log.Error("[RedisStore::CreateEntity] Failed to get field schema for %s", fieldName)
			continue
		}

		requests = append(requests, &pb.DatabaseRequest{
			Id:    entityId,
			Field: fieldName,
		})
	}

	if len(requests) > 0 {
		db.Write(requests)
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
		log.Error("[RedisStore::CreateEntity] Failed to marshal entity: %v", err)
		return
	}

	db.client.SAdd(context.Background(), db.keygen.GetEntityTypeKey(entityType), entityId)
	db.client.Set(context.Background(), db.keygen.GetEntityKey(entityId), base64.StdEncoding.EncodeToString(b), 0)

	if parentId != "" {
		parent := db.GetEntity(parentId)
		if parent != nil {
			parent.Children = append(parent.Children, &pb.EntityReference{Raw: entityId})
			db.SetEntity(parentId, parent)
		} else {
			log.Error("[RedisStore::CreateEntity] Failed to get parent entity: %v", parentId)
		}
	}
}

func (db *RedisStore) GetEntity(entityId string) *pb.DatabaseEntity {
	e, err := db.client.Get(context.Background(), db.keygen.GetEntityKey(entityId)).Result()
	if err != nil {
		log.Error("[RedisStore::GetEntity] Failed to get entity: %v", err)
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(e)
	if err != nil {
		log.Error("[RedisStore::GetEntity] Failed to decode entity: %v", err)
		return nil
	}

	p := &pb.DatabaseEntity{}
	err = proto.Unmarshal(b, p)
	if err != nil {
		log.Error("[RedisStore::GetEntity] Failed to unmarshal entity: %v", err)
		return nil
	}

	return p
}

func (db *RedisStore) SetEntity(entityId string, value *pb.DatabaseEntity) {
	b, err := proto.Marshal(value)
	if err != nil {
		log.Error("[RedisStore::SetEntity] Failed to marshal entity: %v", err)
		return
	}

	err = db.client.Set(context.Background(), db.keygen.GetEntityKey(entityId), base64.StdEncoding.EncodeToString(b), 0).Err()
	if err != nil {
		log.Error("[RedisStore::SetEntity] Failed to set entity '%s': %v", entityId, err)
		return
	}
}

func (db *RedisStore) DeleteEntity(entityId string) {
	p := db.GetEntity(entityId)
	if p == nil {
		log.Error("[RedisStore::DeleteEntity] Failed to get entity: %v", entityId)
		return
	}

	parent := db.GetEntity(p.Parent.Raw)
	if parent != nil {
		newChildren := []*pb.EntityReference{}
		for _, child := range parent.Children {
			if child.Raw != entityId {
				newChildren = append(newChildren, child)
			}
		}
		parent.Children = newChildren
		db.SetEntity(p.Parent.Raw, parent)
	}

	for _, child := range p.Children {
		db.DeleteEntity(child.Raw)
	}

	for _, fieldName := range db.GetEntitySchema(p.Type).Fields {
		db.client.Del(context.Background(), db.keygen.GetFieldKey(fieldName, entityId))
	}

	db.client.SRem(context.Background(), db.keygen.GetEntityTypeKey(p.Type), entityId)
	db.client.Del(context.Background(), db.keygen.GetEntityKey(entityId))
}

func (db *RedisStore) FindEntities(entityType string) []string {
	return db.client.SMembers(context.Background(), db.keygen.GetEntityTypeKey(entityType)).Val()
}

func (db *RedisStore) EntityExists(entityId string) bool {
	e, err := db.client.Get(context.Background(), db.keygen.GetEntityKey(entityId)).Result()
	if err != nil {
		return false
	}

	return e != ""
}

func (db *RedisStore) FieldExists(fieldName, entityType string) bool {
	if !strings.Contains(entityType, "-") {
		schema := db.GetEntitySchema(entityType)
		if schema != nil {
			for _, field := range schema.Fields {
				if field == fieldName {
					return true
				}
			}
			return false
		}
	}

	request := &pb.DatabaseRequest{
		Id:    entityType,
		Field: fieldName,
	}
	db.Read([]*pb.DatabaseRequest{request})

	return request.Success
}

func (db *RedisStore) GetFieldSchemas() []*pb.DatabaseFieldSchema {
	it := db.client.Scan(context.Background(), 0, db.keygen.GetFieldSchemaKey("*"), 0).Iterator()
	schemas := []*pb.DatabaseFieldSchema{}

	for it.Next(context.Background()) {
		e, err := db.client.Get(context.Background(), it.Val()).Result()
		if err != nil {
			log.Error("[RedisStore::GetFieldSchemas] Failed to get field schema: %v", err)
			continue
		}

		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[RedisStore::GetFieldSchemas] Failed to decode field schema: %v", err)
			continue
		}

		p := &pb.DatabaseFieldSchema{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[RedisStore::GetFieldSchemas] Failed to unmarshal field schema: %v", err)
			continue
		}

		schemas = append(schemas, p)
	}

	return schemas

}

func (db *RedisStore) GetFieldSchema(fieldName string) *pb.DatabaseFieldSchema {
	e, err := db.client.Get(context.Background(), db.keygen.GetFieldSchemaKey(fieldName)).Result()
	if err != nil {
		log.Error("[RedisStore::GetFieldSchema] Failed to get field schema: %v", err)
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(e)
	if err != nil {
		log.Error("[RedisStore::GetFieldSchema] Failed to decode field schema: %v", err)
		return nil
	}

	a := &pb.DatabaseFieldSchema{}
	err = proto.Unmarshal(b, a)
	if err != nil {
		log.Error("[RedisStore::GetFieldSchema] Failed to unmarshal field schema: %v", err)
		return nil
	}

	return a
}

func (db *RedisStore) SetFieldSchema(fieldName string, value *pb.DatabaseFieldSchema) {
	b, err := proto.Marshal(value)
	if err != nil {
		log.Error("[RedisStore::SetFieldSchema] Failed to marshal field schema: %v", err)
		return
	}

	db.client.Set(context.Background(), db.keygen.GetFieldSchemaKey(fieldName), base64.StdEncoding.EncodeToString(b), 0)
}

func (db *RedisStore) GetEntityTypes() []string {
	it := db.client.Scan(context.Background(), 0, db.keygen.GetEntitySchemaKey("*"), 0).Iterator()
	types := []string{}

	for it.Next(context.Background()) {
		types = append(types, strings.ReplaceAll(it.Val(), db.keygen.GetEntitySchemaKey(""), ""))
	}

	return types
}

func (db *RedisStore) GetEntitySchema(entityType string) *pb.DatabaseEntitySchema {
	e, err := db.client.Get(context.Background(), db.keygen.GetEntitySchemaKey(entityType)).Result()
	if err != nil {
		log.Error("[RedisStore::GetEntitySchema] Failed to get entity schema (%v): %v", entityType, err)
		return nil
	}

	b, err := base64.StdEncoding.DecodeString(e)
	if err != nil {
		log.Error("[RedisStore::GetEntitySchema] Failed to decode entity schema (%v): %v", entityType, err)
		return nil
	}

	p := &pb.DatabaseEntitySchema{}
	err = proto.Unmarshal(b, p)
	if err != nil {
		log.Error("[RedisStore::GetEntitySchema] Failed to unmarshal entity schema (%v): %v", entityType, err)
		return nil
	}

	return p
}

func (db *RedisStore) SetEntitySchema(entityType string, value *pb.DatabaseEntitySchema) {
	b, err := proto.Marshal(value)
	if err != nil {
		log.Error("[RedisStore::SetEntitySchema] Failed to marshal entity schema: %v", err)
		return
	}

	oldSchema := db.GetEntitySchema(entityType)
	if oldSchema != nil {
		removedFields := []string{}
		newFields := []string{}

		for _, field := range oldSchema.Fields {
			if !slices.Contains(value.Fields, field) {
				removedFields = append(removedFields, field)
			}
		}

		for _, field := range value.Fields {
			if !slices.Contains(oldSchema.Fields, field) {
				newFields = append(newFields, field)
			}
		}

		for _, entityId := range db.FindEntities(entityType) {
			for _, field := range removedFields {
				db.client.Del(context.Background(), db.keygen.GetFieldKey(field, entityId))
			}

			for _, field := range newFields {
				request := &pb.DatabaseRequest{
					Id:    entityId,
					Field: field,
				}
				db.Write([]*pb.DatabaseRequest{request})
			}
		}
	}

	db.client.Set(context.Background(), db.keygen.GetEntitySchemaKey(entityType), base64.StdEncoding.EncodeToString(b), 0)
}

func (db *RedisStore) Read(requests []*pb.DatabaseRequest) {
	for _, request := range requests {
		request.Success = false

		indirectField, indirectEntity := db.ResolveIndirection(request.Field, request.Id)

		if indirectField == "" || indirectEntity == "" {
			log.Error("[RedisStore::Read] Failed to resolve indirection: %v", request)
			continue
		}

		e, err := db.client.Get(context.Background(), db.keygen.GetFieldKey(indirectField, indirectEntity)).Result()
		if err != nil {
			if err != redis.Nil {
				log.Error("[RedisStore::Read] Failed to read field: %v", err)
			} else {
				// If we can't read because the key doesn't exist, it's not a necessarily an issue.
				// It would be good to know from a troubleshooting aspect though.
				log.Trace("[RedisStore::Read] Failed to read field: %v", err)
			}
			continue
		}

		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[RedisStore::Read] Failed to decode field: %v", err)
			continue
		}

		p := &pb.DatabaseField{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[RedisStore::Read] Failed to unmarshal field: %v", err)
			continue
		}

		request.Value = p.Value

		if request.WriteTime == nil {
			request.WriteTime = &pb.Timestamp{Raw: timestamppb.Now()}
		}
		request.WriteTime.Raw = p.WriteTime

		if request.WriterId == nil {
			request.WriterId = &pb.String{Raw: ""}
		}
		request.WriterId.Raw = p.WriterId

		request.Success = true
	}
}

func (db *RedisStore) Write(requests []*pb.DatabaseRequest) {
	for _, request := range requests {
		request.Success = false

		indirectField, indirectEntity := db.ResolveIndirection(request.Field, request.Id)
		if indirectField == "" || indirectEntity == "" {
			log.Error("[RedisStore::Write] Failed to resolve indirection: %v", request)
			continue
		}

		schema := db.GetFieldSchema(indirectField)
		if schema == nil {
			log.Error("[RedisStore::Write] Failed to get field schema for %s", indirectField)
			continue
		}

		actualFieldType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(schema.Type))
		if err != nil {
			log.Error("[RedisStore::Write] Failed to find message type %s: %v", schema.Type, err)
			continue
		}

		if request.Value == nil {
			if request.Value, err = anypb.New(actualFieldType.New().Interface()); err != nil {
				log.Error("[RedisStore::Write] Failed to create anypb: %v", err)
				continue
			}
		} else {
			sampleAnyType, err := anypb.New(actualFieldType.New().Interface())
			if err != nil {
				log.Error("[RedisStore::Write] Failed to create anypb: %v", err)
				continue
			}

			if request.Value.TypeUrl != sampleAnyType.TypeUrl && !sampleAnyType.MessageIs(&pb.Transformation{}) {
				Warn("[RedisStore::Write] Field type mismatch for %s.%s. Got: %v, Expected: %v. Writing default value instead.", request.Id, request.Field, request.Value.TypeUrl, sampleAnyType.TypeUrl)
				request.Value = sampleAnyType
			}
		}

		if request.WriteTime == nil {
			request.WriteTime = &pb.Timestamp{Raw: timestamppb.Now()}
		}

		if request.WriterId == nil {
			request.WriterId = &pb.String{Raw: ""}
		}

		oldRequest := &pb.DatabaseRequest{
			Id:    request.Id,
			Field: request.Field,
		}
		db.Read([]*pb.DatabaseRequest{oldRequest})

		// Set the value in the database
		// Note that for a transformation, we don't actually write the value to the database
		// unless the new value is a transformation. This is because the transformation is
		// executed by the transformer, which will write the result to the database.
		if oldRequest.Success && oldRequest.Value.MessageIs(&pb.Transformation{}) && !request.Value.MessageIs(&pb.Transformation{}) {
			transformation := ValueCast[*pb.Transformation](oldRequest.Value)
			field := NewField(db, request.Id, request.Field)
			field.req = &pb.DatabaseRequest{
				Id:      request.Id,
				Field:   request.Field,
				Value:   request.Value,
				Success: true,
			}
			db.transformer.Transform(transformation, field)
			request.Value = oldRequest.Value
		}

		p := new(pb.DatabaseField).FromRequest(request)

		b, err := proto.Marshal(p)
		if err != nil {
			log.Error("[RedisStore::Write] Failed to marshal field: %v", err)
			continue
		}

		p.Id = indirectEntity
		p.Name = indirectField

		_, err = db.client.Set(context.Background(), db.keygen.GetFieldKey(indirectField, indirectEntity), base64.StdEncoding.EncodeToString(b), 0).Result()

		// Notify listeners of the change
		db.triggerNotifications(request, oldRequest)

		if err != nil {
			log.Error("[RedisStore::Write] Failed to write field: %v", err)
			continue
		}
		request.Success = true
	}
}

func (db *RedisStore) Notify(notification *pb.DatabaseNotificationConfig, callback NotificationCallback) INotificationToken {
	if notification.ServiceId == "" {
		notification.ServiceId = db.getServiceId()
	}

	b, err := proto.Marshal(notification)
	if err != nil {
		log.Error("[RedisStore::Notify] Failed to marshal notification config: %v", err)
		return &NotificationToken{
			db:             db,
			subscriptionId: "",
			callback:       nil,
		}
	}

	e := base64.StdEncoding.EncodeToString(b)

	if db.lastStreamMessageId == "$" {
		r, err := db.client.XInfoStream(context.Background(), db.keygen.GetNotificationChannelKey(db.getServiceId())).Result()
		if err != nil {
			db.lastStreamMessageId = "0"
		} else {
			db.lastStreamMessageId = r.LastGeneratedID
		}
	}

	if notification.Id != "" && db.FieldExists(notification.Field, notification.Id) {
		db.client.SAdd(context.Background(), db.keygen.GetEntityIdNotificationConfigKey(notification.Id, notification.Field), e)
		db.callbacks[e] = append(db.callbacks[e], callback)
		return &NotificationToken{
			db:             db,
			subscriptionId: e,
			callback:       callback,
		}
	}

	if notification.Type != "" && db.FieldExists(notification.Field, notification.Type) {
		db.client.SAdd(context.Background(), db.keygen.GetEntityTypeNotificationConfigKey(notification.Type, notification.Field), e)
		db.callbacks[e] = append(db.callbacks[e], callback)
		return &NotificationToken{
			db:             db,
			subscriptionId: e,
			callback:       callback,
		}
	}

	log.Warn("[RedisStore::Notify] Failed to find field: %v", notification)
	return &NotificationToken{
		db:             db,
		subscriptionId: "",
		callback:       nil,
	}
}

func (db *RedisStore) Unnotify(e string) {
	if db.callbacks[e] == nil {
		log.Warn("[RedisStore::Unnotify] Failed to find callback: %v", e)
		return
	}

	delete(db.callbacks, e)
}

func (db *RedisStore) UnnotifyCallback(e string, c NotificationCallback) {
	if db.callbacks[e] == nil {
		log.Warn("[RedisStore::UnnotifyCallback] Failed to find callback: %v", e)
		return
	}

	callbacks := []NotificationCallback{}
	for _, callback := range db.callbacks[e] {
		if callback.Id() != c.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	db.callbacks[e] = callbacks
}

func (db *RedisStore) ProcessNotifications() {
	db.transformer.ProcessPending()

	r, err := db.client.XRead(context.Background(), &redis.XReadArgs{
		Streams: []string{db.keygen.GetNotificationChannelKey(db.getServiceId()), db.lastStreamMessageId},
		Count:   1000,
		Block:   -1,
	}).Result()

	if err != nil && err != redis.Nil {
		log.Error("[RedisStore::ProcessNotifications] Failed to read stream %v: %v", db.keygen.GetNotificationChannelKey(db.getServiceId()), err)
		return
	}

	for _, x := range r {
		for _, m := range x.Messages {
			db.lastStreamMessageId = m.ID
			decodedMessage := make(map[string]string)

			for key, value := range m.Values {
				if castedValue, ok := value.(string); ok {
					decodedMessage[key] = castedValue
				} else {
					log.Error("[RedisStore::ProcessNotifications] Failed to cast value: %v", value)
					continue
				}
			}

			if data, ok := decodedMessage["data"]; ok {
				p, err := base64.StdEncoding.DecodeString(data)
				if err != nil {
					log.Error("[RedisStore::ProcessNotifications] Failed to decode notification: %v", err)
					continue
				}

				n := &pb.DatabaseNotification{}
				err = proto.Unmarshal(p, n)
				if err != nil {
					log.Error("[RedisStore::ProcessNotifications] Failed to unmarshal notification: %v", err)
					continue
				}

				for _, callback := range db.callbacks[n.Token] {
					callback.Fn(n)
				}
			}
		}
	}
}

func (db *RedisStore) ResolveIndirection(indirectField, entityId string) (string, string) {
	fields := strings.Split(indirectField, "->")

	if len(fields) == 1 {
		return indirectField, entityId
	}

	for _, field := range fields[:len(fields)-1] {
		request := &pb.DatabaseRequest{
			Id:    entityId,
			Field: field,
		}

		db.Read([]*pb.DatabaseRequest{request})

		if request.Success {
			entityReference := &pb.EntityReference{}
			if request.Value.MessageIs(entityReference) {
				err := request.Value.UnmarshalTo(entityReference)
				if err != nil {
					log.Error("[RedisStore::ResolveIndirection] Failed to unmarshal entity reference: %v", err)
					return "", ""
				}

				entityId = entityReference.Raw
				continue
			}

			log.Error("[RedisStore::ResolveIndirection] Field is not an entity reference: %v", request)
			return "", ""
		}

		// Fallback to parent entity reference by name
		entity := db.GetEntity(entityId)
		if entity == nil {
			log.Error("[RedisStore::ResolveIndirection] Failed to get entity: %v", entityId)
			return "", ""
		}

		if entity.Parent != nil && entity.Parent.Raw != "" {
			parentEntity := db.GetEntity(entity.Parent.Raw)

			if parentEntity != nil && parentEntity.Name == field {
				entityId = entity.Parent.Raw
				continue
			}
		}

		// Fallback to child entity reference by name
		foundChild := false
		for _, child := range entity.Children {
			childEntity := db.GetEntity(child.Raw)
			if childEntity == nil {
				log.Error("[RedisStore::ResolveIndirection] Failed to get child entity: %v", child.Raw)
				continue
			}

			if childEntity.Name == field {
				entityId = child.Raw
				foundChild = true
				break
			}
		}

		if !foundChild {
			log.Error("[RedisStore::ResolveIndirection] Failed to find child entity: %v", field)
			return "", ""
		}
	}

	return fields[len(fields)-1], entityId
}

func (db *RedisStore) triggerNotifications(request *pb.DatabaseRequest, oldRequest *pb.DatabaseRequest) {
	// failed to read old value (it may not exist initially)
	if !oldRequest.Success {
		log.Warn("[RedisStore::triggerNotifications] Failed to read old value: %v", oldRequest)
		return
	}

	changed := !proto.Equal(request.Value, oldRequest.Value)

	indirectField, indirectEntity := db.ResolveIndirection(request.Field, request.Id)

	if indirectField == "" || indirectEntity == "" {
		log.Error("[RedisStore::triggerNotifications] Failed to resolve indirection: %v", request)
		return
	}

	m, err := db.client.SMembers(context.Background(), db.keygen.GetEntityIdNotificationConfigKey(indirectEntity, indirectField)).Result()
	if err != nil {
		log.Error("[RedisStore::triggerNotifications] Failed to get notification config: %v", err)
		return
	}

	for _, e := range m {
		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to decode notification config: %v", err)
			continue
		}

		p := &pb.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to unmarshal notification config: %v", err)
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
			db.Read([]*pb.DatabaseRequest{contextRequest})
			if contextRequest.Success {
				n.Context = append(n.Context, new(pb.DatabaseField).FromRequest(contextRequest))
			}
		}

		b, err = proto.Marshal(n)
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to marshal notification: %v", err)
			continue
		}

		_, err = db.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: db.keygen.GetNotificationChannelKey(p.ServiceId),
			Values: []string{"data", base64.StdEncoding.EncodeToString(b)},
			MaxLen: 1000,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to add notification: %v", err)
			continue
		}
	}

	entity := db.GetEntity(indirectEntity)
	if entity == nil {
		log.Error("[RedisStore::triggerNotifications] Failed to get entity: %v (indirect=%v)", request.Id, indirectEntity)
		return
	}

	m, err = db.client.SMembers(context.Background(), db.keygen.GetEntityTypeNotificationConfigKey(entity.Type, indirectField)).Result()
	if err != nil {
		log.Error("[RedisStore::triggerNotifications] Failed to get notification config: %v", err)
		return
	}

	for _, e := range m {
		b, err := base64.StdEncoding.DecodeString(e)
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to decode notification config: %v", err)
			continue
		}

		p := &pb.DatabaseNotificationConfig{}
		err = proto.Unmarshal(b, p)
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to unmarshal notification config: %v", err)
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
			db.Read([]*pb.DatabaseRequest{contextRequest})
			if contextRequest.Success {
				n.Context = append(n.Context, new(pb.DatabaseField).FromRequest(contextRequest))
			}
		}

		b, err = proto.Marshal(n)
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to marshal notification: %v", err)
			continue
		}

		_, err = db.client.XAdd(context.Background(), &redis.XAddArgs{
			Stream: db.keygen.GetNotificationChannelKey(p.ServiceId),
			Values: []string{"data", base64.StdEncoding.EncodeToString(b)},
			MaxLen: 100,
			Approx: true,
		}).Result()
		if err != nil {
			log.Error("[RedisStore::triggerNotifications] Failed to add notification: %v", err)
			continue
		}
	}
}

func (db *RedisStore) TempSet(key, value string, expiration time.Duration) bool {
	r, err := db.client.SetNX(context.Background(), key, value, expiration).Result()
	if err != nil {
		return false
	}

	return r
}

func (db *RedisStore) TempGet(key string) string {
	r, err := db.client.Get(context.Background(), key).Result()
	if err != nil {
		return ""
	}

	return r
}

func (db *RedisStore) TempExpire(key string, expiration time.Duration) {
	db.client.Expire(context.Background(), key, expiration)
}

func (db *RedisStore) TempDel(key string) {
	db.client.Del(context.Background(), key)
}

func (db *RedisStore) SortedSetAdd(key string, member string, score float64) int64 {
	result, err := db.client.ZAdd(context.Background(), key, redis.Z{
		Score:  score,
		Member: member,
	}).Result()
	if err != nil {
		log.Error("[RedisStore::SortedSetAdd] Failed to add member to sorted set: %v", err)
		return 0
	}
	return result
}

func (db *RedisStore) SortedSetRemove(key string, member string) int64 {
	result, err := db.client.ZRem(context.Background(), key, member).Result()
	if err != nil {
		log.Error("[RedisStore::SortedSetRemove] Failed to remove member from sorted set: %v", err)
		return 0
	}
	return result
}

func (db *RedisStore) SortedSetRemoveRangeByRank(key string, start, stop int64) int64 {
	result, err := db.client.ZRemRangeByRank(context.Background(), key, start, stop).Result()
	if err != nil {
		log.Error("[RedisStore::SortedSetRemoveRangeByRank] Failed to remove range from sorted set: %v", err)
		return 0
	}
	return result
}

func (db *RedisStore) SortedSetRangeByScoreWithScores(key string, min, max string) []SortedSetMember {
	result, err := db.client.ZRangeByScoreWithScores(context.Background(), key, &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		log.Error("[RedisStore::SortedSetRangeByScoreWithScores] Failed to get range from sorted set: %v", err)
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
