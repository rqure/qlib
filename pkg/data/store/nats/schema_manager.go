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

func (s *SchemaManager) SetEntityManager(em data.EntityManager) {
	s.entityManager = em
}

func (s *SchemaManager) SetFieldOperator(fo data.FieldOperator) {
	s.fieldOperator = fo
}

func (s *SchemaManager) EntityExists(ctx context.Context, entityId string) bool {
	msg := &protobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId,
	}

	resp, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response protobufs.ApiRuntimeEntityExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

func (s *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	msg := &protobufs.ApiRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
	}

	resp, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response protobufs.ApiRuntimeFieldExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

func (s *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	msg := &protobufs.ApiConfigGetEntitySchemaRequest{
		Type: entityType,
	}

	resp, err := s.core.Request(ctx, s.core.GetKeyGenerator().GetReadSubject(), msg)
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

func (s *SchemaManager) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	msg := &protobufs.ApiConfigSetEntitySchemaRequest{
		Schema: entity.ToSchemaPb(schema),
	}

	s.core.Publish(s.core.GetKeyGenerator().GetWriteSubject(), msg)
}

func (s *SchemaManager) GetFieldSchema(ctx context.Context, fieldName, entityType string) data.FieldSchema {
	schema := s.GetEntitySchema(ctx, entityType)
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

func (s *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	entitySchema := s.GetEntitySchema(ctx, entityType)
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
	s.SetEntitySchema(ctx, entitySchema)
}
