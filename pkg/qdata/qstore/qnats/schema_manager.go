package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type SchemaManager struct {
	core          Core
	entityManager qdata.EntityManager
	fieldOperator qdata.FieldOperator
}

func NewSchemaManager(core Core) qdata.ModifiableSchemaManager {
	return &SchemaManager{core: core}
}

func (me *SchemaManager) SetEntityManager(em qdata.EntityManager) {
	me.entityManager = em
}

func (me *SchemaManager) SetFieldOperator(fo qdata.FieldOperator) {
	me.fieldOperator = fo
}

func (me *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	msg := &qprotobufs.ApiRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response qprotobufs.ApiRuntimeFieldExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

func (me *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) qdata.ModifiableEntitySchema {
	msg := &qprotobufs.ApiConfigGetEntitySchemaRequest{
		Type: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigGetEntitySchemaResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != qprotobufs.ApiConfigGetEntitySchemaResponse_SUCCESS {
		return nil
	}

	return qentity.FromSchemaPb(response.Schema)
}

func (me *SchemaManager) SetEntitySchema(ctx context.Context, schema qdata.EntitySchema) {
	msg := &qprotobufs.ApiConfigSetEntitySchemaRequest{
		Schema: qentity.ToSchemaPb(schema),
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to set entity schema: %v", err)
	}
}

func (me *SchemaManager) GetFieldSchema(ctx context.Context, entityType, fieldName string) qdata.FieldSchema {
	schema := me.GetEntitySchema(ctx, entityType)
	if schema == nil {
		return nil
	}

	return schema.GetField(fieldName)
}

func (me *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema qdata.FieldSchema) {
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		qlog.Error("Failed to get entity schema for type %s", entityType)
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
