package qdata

type Snapshot interface {
	GetEntities() []*Entity
	GetFields() []*Field
	GetSchemas() []*EntitySchema

	SetEntities([]*Entity)
	SetFields([]*Field)
	SetSchemas([]*EntitySchema)

	AppendEntity(*Entity)
	AppendField(*Field)
	AppendSchema(*EntitySchema)
}
