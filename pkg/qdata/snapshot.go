package qdata

type Snapshot struct {
	Entities []*Entity
	Fields   []*Field
	Schemas  []*EntitySchema
}

type SnapshotOpts func(*Snapshot)

func SOEntities(entities ...*Entity) SnapshotOpts {
	return func(me *Snapshot) {
		me.Entities = entities
	}
}

func SOFields(fields ...*Field) SnapshotOpts {
	return func(me *Snapshot) {
		me.Fields = fields
	}
}

func SOSchemas(schemas ...*EntitySchema) SnapshotOpts {
	return func(me *Snapshot) {
		me.Schemas = schemas
	}
}

func (me *Snapshot) Init(opts ...SnapshotOpts) *Snapshot {
	me.Entities = make([]*Entity, 0)
	me.Fields = make([]*Field, 0)
	me.Schemas = make([]*EntitySchema, 0)

	return me.ApplyOpts(opts...)
}

func (me *Snapshot) ApplyOpts(opts ...SnapshotOpts) *Snapshot {
	for _, opt := range opts {
		opt(me)
	}

	return me
}
