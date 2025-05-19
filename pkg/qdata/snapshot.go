package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

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

func (me *Snapshot) FromSnapshotPb(pb *qprotobufs.DatabaseSnapshot) *Snapshot {
	me.Init()

	for _, entity := range pb.Entities {
		me.Entities = append(me.Entities, new(Entity).FromEntityPb(entity))
	}

	for _, field := range pb.Fields {
		me.Fields = append(me.Fields, new(Field).FromFieldPb(field))
	}

	for _, schema := range pb.EntitySchemas {
		me.Schemas = append(me.Schemas, new(EntitySchema).FromEntitySchemaPb(schema))
	}

	return me
}

func (me *Snapshot) AsSnapshotPb() *qprotobufs.DatabaseSnapshot {
	pb := &qprotobufs.DatabaseSnapshot{
		Entities:      make([]*qprotobufs.DatabaseEntity, len(me.Entities)),
		Fields:        make([]*qprotobufs.DatabaseField, len(me.Fields)),
		EntitySchemas: make([]*qprotobufs.DatabaseEntitySchema, len(me.Schemas)),
	}

	for i, entity := range me.Entities {
		pb.Entities[i] = entity.AsEntityPb()
	}

	for i, field := range me.Fields {
		pb.Fields[i] = field.AsFieldPb()
	}

	for i, schema := range me.Schemas {
		pb.EntitySchemas[i] = schema.AsEntitySchemaPb()
	}

	return pb
}

func (me *Snapshot) AsBytes() ([]byte, error) {
	return proto.Marshal(me.AsSnapshotPb())
}

func (me *Snapshot) FromBytes(data []byte) (*Snapshot, error) {
	pb := &qprotobufs.DatabaseSnapshot{}
	if err := proto.Unmarshal(data, pb); err != nil {
		return nil, err
	}

	return me.FromSnapshotPb(pb), nil
}
