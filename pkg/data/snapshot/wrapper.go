package snapshot

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Wrapper struct {
	impl *protobufs.DatabaseSnapshot
}

func New() data.Snapshot {
	return &Wrapper{
		impl: &protobufs.DatabaseSnapshot{},
	}
}

func ToPb(s data.Snapshot) *protobufs.DatabaseSnapshot {
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

func FromPb(impl *protobufs.DatabaseSnapshot) data.Snapshot {
	return &Wrapper{
		impl: impl,
	}
}

func (w *Wrapper) GetEntities() []data.Entity {
	entities := make([]data.Entity, len(w.impl.Entities))
	for i, e := range w.impl.Entities {
		entities[i] = entity.FromEntityPb(e)
	}

	return entities
}

func (w *Wrapper) GetFields() []data.Field {
	fields := make([]data.Field, len(w.impl.Fields))
	for i, f := range w.impl.Fields {
		fields[i] = field.FromFieldPb(f)
	}

	return fields
}

func (w *Wrapper) GetSchemas() []data.EntitySchema {
	schemas := make([]data.EntitySchema, len(w.impl.EntitySchemas))
	for i, s := range w.impl.EntitySchemas {
		schemas[i] = entity.FromSchemaPb(s)
	}

	return schemas
}

func (w *Wrapper) SetEntities(entities []data.Entity) {
	w.impl.Entities = make([]*protobufs.DatabaseEntity, len(entities))
	for i, e := range entities {
		w.impl.Entities[i] = entity.ToEntityPb(e)
	}
}

func (w *Wrapper) SetFields(fields []data.Field) {
	w.impl.Fields = make([]*protobufs.DatabaseField, len(fields))
	for i, f := range fields {
		w.impl.Fields[i] = field.ToFieldPb(f)
	}
}

func (w *Wrapper) SetSchemas(schemas []data.EntitySchema) {
	w.impl.EntitySchemas = make([]*protobufs.DatabaseEntitySchema, len(schemas))
	for i, s := range schemas {
		w.impl.EntitySchemas[i] = entity.ToSchemaPb(s)
	}
}

func (w *Wrapper) AppendEntity(e data.Entity) {
	w.impl.Entities = append(w.impl.Entities, entity.ToEntityPb(e))
}

func (w *Wrapper) AppendField(f data.Field) {
	w.impl.Fields = append(w.impl.Fields, field.ToFieldPb(f))
}

func (w *Wrapper) AppendSchema(s data.EntitySchema) {
	w.impl.EntitySchemas = append(w.impl.EntitySchemas, entity.ToSchemaPb(s))
}
