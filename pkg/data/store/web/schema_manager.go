package web

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
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
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeEntityExistsRequest{
		EntityId: entityId,
	})

	response := s.core.SendAndWait(ctx, msg)
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

func (s *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeFieldExistsRequest{
		FieldName:  fieldName,
		EntityType: entityType,
	})

	response := s.core.SendAndWait(ctx, msg)
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

func (s *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigGetEntitySchemaRequest{
		Type: entityType,
	})

	response := s.core.SendAndWait(ctx, msg)
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

func (s *SchemaManager) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebConfigSetEntitySchemaRequest{
		Schema: entity.ToSchemaPb(schema),
	})

	s.core.SendAndWait(ctx, msg)
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
