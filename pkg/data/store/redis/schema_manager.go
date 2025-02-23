package redis

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type SchemaManager struct {
	core          Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewSchemaManager(core Core) data.ModifiableSchemaManager {
	return &SchemaManager{core: core}
}

func (me *SchemaManager) SetEntityManager(manager data.EntityManager) {
	me.entityManager = manager
}

func (me *SchemaManager) SetFieldOperator(operator data.FieldOperator) {
	me.fieldOperator = operator
}

func (me *SchemaManager) EntityExists(ctx context.Context, entityId string) bool {
	exists, _ := me.core.GetClient().Exists(ctx, me.core.GetKeyGen().GetEntityKey(entityId)).Result()
	return exists > 0
}

func (me *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	if !strings.Contains(entityType, "-") {
		schema := me.GetEntitySchema(ctx, entityType)
		if schema != nil {
			return schema.GetField(fieldName) != nil
		}
		return false
	}

	r := request.New()
	r.SetEntityId(entityType)
	r.SetFieldName(fieldName)

	me.fieldOperator.Read(ctx, r)
	return r.IsSuccessful()
}

func (me *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	schemaBytes, err := me.core.GetClient().Get(ctx, me.core.GetKeyGen().GetEntitySchemaKey(entityType)).Bytes()
	if err != nil {
		return nil
	}

	schemaPb := &protobufs.DatabaseEntitySchema{}
	if err := proto.Unmarshal(schemaBytes, schemaPb); err != nil {
		log.Error("Failed to unmarshal schema: %v", err)
		return nil
	}

	return entity.FromSchemaPb(schemaPb)
}

func (me *SchemaManager) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	schemaPb := entity.ToSchemaPb(schema)
	schemaBytes, err := proto.Marshal(schemaPb)
	if err != nil {
		log.Error("Failed to marshal schema: %v", err)
		return
	}

	err = me.core.GetClient().Set(ctx, me.core.GetKeyGen().GetEntitySchemaKey(schema.GetType()),
		base64.StdEncoding.EncodeToString(schemaBytes), 0).Err()
	if err != nil {
		log.Error("Failed to save schema: %v", err)
		return
	}

	// Handle field updates for existing entities
	oldSchema := me.GetEntitySchema(ctx, schema.GetType())
	if oldSchema != nil {
		me.updateExistingEntities(ctx, oldSchema, schema)
	}
}

func (me *SchemaManager) GetFieldSchema(ctx context.Context, entityType, fieldName string) data.FieldSchema {
	schema := me.GetEntitySchema(ctx, entityType)
	if schema == nil {
		return nil
	}
	return schema.GetField(fieldName)
}

func (me *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, fieldSchema data.FieldSchema) {
	schema := me.GetEntitySchema(ctx, entityType)
	if schema == nil {
		log.Error("Entity schema not found: %s", entityType)
		return
	}

	fields := schema.GetFields()
	for i, f := range fields {
		if f.GetFieldName() == fieldName {
			fields[i] = fieldSchema
			schema.SetFields(fields)
			me.SetEntitySchema(ctx, schema)
			return
		}
	}

	fields = append(fields, fieldSchema)
	schema.SetFields(fields)
	me.SetEntitySchema(ctx, schema)
}

func (me *SchemaManager) updateExistingEntities(ctx context.Context, oldSchema, newSchema data.EntitySchema) {
	removedFields := []string{}
	newFields := []string{}

	// Find removed and new fields
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

	// Update all entities of this type
	for _, entityId := range me.entityManager.FindEntities(ctx, newSchema.GetType()) {
		// Remove deleted fields
		for _, field := range removedFields {
			me.core.GetClient().Del(ctx, me.core.GetKeyGen().GetFieldKey(field, entityId))
		}

		// Initialize new fields
		for _, field := range newFields {
			r := request.New().SetEntityId(entityId).SetFieldName(field)
			me.fieldOperator.Write(ctx, r)
		}
	}
}
