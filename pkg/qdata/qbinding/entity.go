package qbinding

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
)

type Entity struct {
	impl qdata.Entity

	entityManager qdata.EntityManager
	schemaManager qdata.SchemaManager
	fieldOperator qdata.FieldOperator

	fields map[string]qdata.FieldBinding
}

func NewEntity(ctx context.Context, entityManager qdata.EntityManager, schemaManager qdata.SchemaManager, fieldOperator qdata.FieldOperator, entityId string) qdata.EntityBinding {
	e := entityManager.GetEntity(ctx, entityId)

	if e == nil {
		return nil
	}

	return NewEntityFromImpl(entityManager, schemaManager, fieldOperator, e)
}

func NewEntityFromImpl(entityManager qdata.EntityManager, schemaManager qdata.SchemaManager, fieldOperator qdata.FieldOperator, impl qdata.Entity) qdata.EntityBinding {
	return &Entity{
		impl: impl,

		entityManager: entityManager,
		schemaManager: schemaManager,
		fieldOperator: fieldOperator,

		fields: make(map[string]qdata.FieldBinding),
	}
}

func (e *Entity) GetId() string {
	return e.impl.GetId()
}

func (e *Entity) GetType() string {
	return e.impl.GetType()
}

func (e *Entity) GetField(fieldName string) qdata.FieldBinding {
	return e.fields[fieldName]
}
