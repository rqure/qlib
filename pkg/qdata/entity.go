package qdata

type Entity interface {
	GetId() string
	GetType() string
}

type EntitySchema interface {
	GetType() string
	GetFields() []FieldSchema
	GetFieldNames() []string
	GetField(string) FieldSchema
}

type ModifiableEntity interface {
	Entity

	SetId(string)
	SetType(string)
}

type ModifiableEntitySchema interface {
	EntitySchema

	SetType(string)
	SetFields([]FieldSchema)
	SetField(string, FieldSchema)
}
