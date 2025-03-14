package qsnapshot

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type Wrapper struct {
	impl *qprotobufs.DatabaseSnapshot
}

func New() qdata.Snapshot {
	return &Wrapper{
		impl: &qprotobufs.DatabaseSnapshot{},
	}
}

func ToPb(s qdata.Snapshot) *qprotobufs.DatabaseSnapshot {
	if s == nil {
		return nil
	}

	switch c := s.(type) {
	case *Wrapper:
		return c.impl
	default:
		return nil
	}
}

func FromPb(impl *qprotobufs.DatabaseSnapshot) qdata.Snapshot {
	return &Wrapper{
		impl: impl,
	}
}

func (w *Wrapper) GetEntities() []qdata.Entity {
	entities := make([]qdata.Entity, len(w.impl.Entities))
	for i, e := range w.impl.Entities {
		entities[i] = qentity.FromEntityPb(e)
	}

	return entities
}

func (w *Wrapper) GetFields() []qdata.Field {
	fields := make([]qdata.Field, len(w.impl.Fields))
	for i, f := range w.impl.Fields {
		fields[i] = qfield.FromFieldPb(f)
	}

	return fields
}

func (w *Wrapper) GetSchemas() []qdata.EntitySchema {
	schemas := make([]qdata.EntitySchema, len(w.impl.EntitySchemas))
	for i, s := range w.impl.EntitySchemas {
		schemas[i] = qentity.FromSchemaPb(s)
	}

	return schemas
}

func (w *Wrapper) SetEntities(entities []qdata.Entity) {
	w.impl.Entities = make([]*qprotobufs.DatabaseEntity, len(entities))
	for i, e := range entities {
		w.impl.Entities[i] = qentity.ToEntityPb(e)
	}
}

func (w *Wrapper) SetFields(fields []qdata.Field) {
	w.impl.Fields = make([]*qprotobufs.DatabaseField, len(fields))
	for i, f := range fields {
		w.impl.Fields[i] = qfield.ToFieldPb(f)
	}
}

func (w *Wrapper) SetSchemas(schemas []qdata.EntitySchema) {
	w.impl.EntitySchemas = make([]*qprotobufs.DatabaseEntitySchema, len(schemas))
	for i, s := range schemas {
		w.impl.EntitySchemas[i] = qentity.ToSchemaPb(s)
	}
}

func (w *Wrapper) AppendEntity(e qdata.Entity) {
	w.impl.Entities = append(w.impl.Entities, qentity.ToEntityPb(e))
}

func (w *Wrapper) AppendField(f qdata.Field) {
	w.impl.Fields = append(w.impl.Fields, qfield.ToFieldPb(f))
}

func (w *Wrapper) AppendSchema(s qdata.EntitySchema) {
	w.impl.EntitySchemas = append(w.impl.EntitySchemas, qentity.ToSchemaPb(s))
}
