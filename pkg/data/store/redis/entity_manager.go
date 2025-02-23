package redis

import (
	"context"
	"encoding/base64"

	"github.com/google/uuid"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type EntityManager struct {
	core          Core
	schemaManager data.SchemaManager
	fieldOperator data.FieldOperator
}

func NewEntityManager(core Core) data.ModifiableEntityManager {
	return &EntityManager{core: core}
}

func (me *EntityManager) SetSchemaManager(manager data.SchemaManager) {
	me.schemaManager = manager
}

func (me *EntityManager) SetFieldOperator(operator data.FieldOperator) {
	me.fieldOperator = operator
}

func (me *EntityManager) GetEntity(ctx context.Context, entityId string) data.Entity {
	entityBytes, err := me.core.GetClient().Get(ctx, me.core.GetKeyGen().GetEntityKey(entityId)).Bytes()
	if err != nil {
		return nil
	}

	entityPb := &protobufs.DatabaseEntity{}
	if err := proto.Unmarshal(entityBytes, entityPb); err != nil {
		log.Error("Failed to unmarshal entity: %v", err)
		return nil
	}

	return entity.FromEntityPb(entityPb)
}

func (me *EntityManager) SetEntity(ctx context.Context, e data.Entity) {
	entityPb := entity.ToEntityPb(e)
	entityBytes, err := proto.Marshal(entityPb)
	if err != nil {
		log.Error("Failed to marshal entity: %v", err)
		return
	}

	err = me.core.GetClient().Set(ctx, me.core.GetKeyGen().GetEntityKey(e.GetId()),
		base64.StdEncoding.EncodeToString(entityBytes), 0).Err()
	if err != nil {
		log.Error("Failed to save entity: %v", err)
		return
	}

	me.core.GetClient().SAdd(ctx, me.core.GetKeyGen().GetEntityTypeKey(e.GetType()), e.GetId())
}

func (me *EntityManager) CreateEntity(ctx context.Context, entityType, parentId, name string) {
	entityId := uuid.New().String()
	e := &protobufs.DatabaseEntity{
		Id:       entityId,
		Name:     name,
		Parent:   &protobufs.EntityReference{Raw: parentId},
		Type:     entityType,
		Children: []*protobufs.EntityReference{},
	}

	me.SetEntity(ctx, entity.FromEntityPb(e))

	if parentId != "" {
		if parent := me.GetEntity(ctx, parentId); parent != nil {
			parent.AppendChildId(entityId)
			me.SetEntity(ctx, parent)
		}
	}

	// Initialize fields with default values
	if schema := me.schemaManager.GetEntitySchema(ctx, entityType); schema != nil {
		for _, field := range schema.GetFields() {
			r := request.New().
				SetEntityId(entityId).
				SetFieldName(field.GetFieldName())
			me.fieldOperator.Write(ctx, r)
		}
	}
}

func (me *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	e := me.GetEntity(ctx, entityId)
	if e == nil {
		return
	}

	// Delete children recursively
	for _, childId := range e.GetChildrenIds() {
		me.DeleteEntity(ctx, childId)
	}

	// Update parent
	if parent := me.GetEntity(ctx, e.GetParentId()); parent != nil {
		parent.RemoveChildId(entityId)
		me.SetEntity(ctx, parent)
	}

	// Delete fields
	schema := me.schemaManager.GetEntitySchema(ctx, e.GetType())
	if schema != nil {
		for _, field := range schema.GetFields() {
			me.core.GetClient().Del(ctx, me.core.GetKeyGen().GetFieldKey(field.GetFieldName(), entityId))
		}
	}

	// Delete notifications
	me.core.GetClient().Del(ctx, me.core.GetKeyGen().GetEntityIdNotificationConfigKey(entityId, "*"))

	// Delete entity
	me.core.GetClient().SRem(ctx, me.core.GetKeyGen().GetEntityTypeKey(e.GetType()), entityId)
	me.core.GetClient().Del(ctx, me.core.GetKeyGen().GetEntityKey(entityId))
}

func (me *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	members, err := me.core.GetClient().SMembers(ctx, me.core.GetKeyGen().GetEntityTypeKey(entityType)).Result()
	if err != nil {
		log.Error("Failed to find entities: %v", err)
		return nil
	}
	return members
}

func (me *EntityManager) GetEntityTypes(ctx context.Context) []string {
	var types []string
	iter := me.core.GetClient().Scan(ctx, 0, me.core.GetKeyGen().GetEntitySchemaKey("*"), 0).Iterator()
	prefix := me.core.GetKeyGen().GetEntitySchemaKey("")

	for iter.Next(ctx) {
		key := iter.Val()
		if len(key) > len(prefix) {
			types = append(types, key[len(prefix):])
		}
	}

	return types
}
