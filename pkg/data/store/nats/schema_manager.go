package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type SchemaManager struct {
	core          Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewSchemaManager(core Core) data.ModifiableSchemaManager {
	return &SchemaManager{core: core}
}

func (me *SchemaManager) SetEntityManager(em data.EntityManager) {
	me.entityManager = em
}

func (me *SchemaManager) SetFieldOperator(fo data.FieldOperator) {
	me.fieldOperator = fo
}

func (me *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	msg := &protobufs.ApiRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response protobufs.ApiRuntimeFieldExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

func (me *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	msg := &protobufs.ApiConfigGetEntitySchemaRequest{
		Type: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response protobufs.ApiConfigGetEntitySchemaResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != protobufs.ApiConfigGetEntitySchemaResponse_SUCCESS {
		return nil
	}

	return entity.FromSchemaPb(response.Schema)
}

func (me *SchemaManager) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	msg := &protobufs.ApiConfigSetEntitySchemaRequest{
		Schema: entity.ToSchemaPb(schema),
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		log.Error("Failed to set entity schema: %v", err)
	}
}

func (me *SchemaManager) GetFieldSchema(ctx context.Context, fieldName, entityType string) data.FieldSchema {
	schema := me.GetEntitySchema(ctx, entityType)
	if schema == nil {
		return nil
	}

	for _, field := range schema.GetFields() {
		if field.GetFieldName() == fieldName {
			return field
		}
	}

	return nil
}

func (me *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		log.Error("Failed to get entity schema for type %s", entityType)
		return
	}

	fields := entitySchema.GetFields()
	updated := false
	for i, f := range fields {
		if f.GetFieldName() == fieldName {
			fields[i] = schema
			updated = true
			break
		}
	}

	if !updated {
		fields = append(fields, schema)
	}

	entitySchema.SetFields(fields)
	me.SetEntitySchema(ctx, entitySchema)
}
