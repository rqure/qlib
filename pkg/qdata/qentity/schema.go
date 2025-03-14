package qentity

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type Schema struct {
	impl *qprotobufs.DatabaseEntitySchema
}

func ToSchemaPb(s qdata.EntitySchema) *qprotobufs.DatabaseEntitySchema {
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

func FromSchemaPb(impl *qprotobufs.DatabaseEntitySchema) qdata.EntitySchema {
	if impl != nil {
		for _, f := range impl.Fields {
			if f == nil {
				qlog.Error("Field is nil")
				continue
			}

			if f.Name == "" {
				qlog.Error("Field name is empty")
				continue
			}

			if f.Type == "" {
				qlog.Error("Field type is empty")
				continue
			}

			f.Type = qfield.PrefixedType(f.Type)
		}
	}

	return &Schema{
		impl: impl,
	}
}

func (s *Schema) GetType() string {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return s.impl.Name
}

func (s *Schema) GetFields() []qdata.FieldSchema {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return []qdata.FieldSchema{}
	}

	fields := make([]qdata.FieldSchema, len(s.impl.Fields))
	for i, f := range s.impl.Fields {
		fields[i] = qfield.FromSchemaPb(f)
	}

	return fields
}

func (s *Schema) GetFieldNames() []string {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return []string{}
	}

	names := make([]string, len(s.impl.Fields))
	for i, f := range s.impl.Fields {
		names[i] = f.Name
	}

	return names
}

func (s *Schema) GetField(name string) qdata.FieldSchema {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return nil
	}

	for _, f := range s.impl.Fields {
		if f.Name == name {
			return qfield.FromSchemaPb(f)
		}
	}

	return nil
}

func (s *Schema) SetFields(fields []qdata.FieldSchema) {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	s.impl.Fields = make([]*qprotobufs.DatabaseFieldSchema, len(fields))
	for i, f := range fields {
		s.impl.Fields[i] = qfield.ToSchemaPb(f)
	}
}

func (s *Schema) SetField(name string, newField qdata.FieldSchema) {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	for i, f := range s.impl.Fields {
		if f.Name == name {
			s.impl.Fields[i] = qfield.ToSchemaPb(newField)
			return
		}
	}

	s.impl.Fields = append(s.impl.Fields, qfield.ToSchemaPb(newField))
}

func (s *Schema) SetType(t string) {
	if s.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	s.impl.Name = t
}
