package entity

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Schema struct {
	impl *protobufs.DatabaseEntitySchema
}

func ToSchemaPb(s data.EntitySchema) *protobufs.DatabaseEntitySchema {
	if s == nil {
		return nil
	}

	switch c := s.(type) {
	case *Schema:
		return c.impl
	default:
		return nil
	}
}

func FromSchemaPb(impl *protobufs.DatabaseEntitySchema) data.EntitySchema {
	return &Schema{
		impl: impl,
	}
}

func (s *Schema) GetType() string {
	if s.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return s.impl.Name
}

func (s *Schema) GetFields() []data.FieldSchema {
	if s.impl == nil {
		log.Error("Impl not defined")
		return []data.FieldSchema{}
	}

	fields := make([]data.FieldSchema, len(s.impl.Fields))
	for i, f := range s.impl.Fields {
		fields[i] = field.FromSchemaPb(f)
	}

	return fields
}

func (s *Schema) GetFieldNames() []string {
	if s.impl == nil {
		log.Error("Impl not defined")
		return []string{}
	}

	names := make([]string, len(s.impl.Fields))
	for i, f := range s.impl.Fields {
		names[i] = f.Name
	}

	return names
}

func (s *Schema) GetField(name string) data.FieldSchema {
	if s.impl == nil {
		log.Error("Impl not defined")
		return nil
	}

	for _, f := range s.impl.Fields {
		if f.Name == name {
			return field.FromSchemaPb(f)
		}
	}

	return nil
}

func (s *Schema) SetFields(fields []data.FieldSchema) {
	if s.impl == nil {
		log.Error("Impl not defined")
		return
	}

	s.impl.Fields = make([]*protobufs.DatabaseFieldSchema, len(fields))
	for i, f := range fields {
		s.impl.Fields[i] = field.ToSchemaPb(f)
	}
}

func (s *Schema) SetType(t string) {
	if s.impl == nil {
		log.Error("Impl not defined")
		return
	}

	s.impl.Name = t
}
