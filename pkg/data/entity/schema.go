package entity

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Schema struct {
	impl *protobufs.DatabaseEntitySchema
}

func FromSchemaPb(impl *protobufs.DatabaseEntitySchema) data.EntitySchema {
	return &Schema{
		impl: impl,
	}
}

func (s *Schema) GetType() string {
	return s.impl.Name
}

func (s *Schema) GetFields() []data.FieldSchema {
	fields := make([]data.FieldSchema, len(s.impl.Fields))
	for i, f := range s.impl.Fields {
		fields[i] = field.FromSchemaPb(f)
	}

	return fields
}
