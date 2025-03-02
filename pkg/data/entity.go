package data

type Entity interface {
	GetId() string
	GetType() string

	SetId(string)
	SetType(string)

	Impl() any
}

type EntitySchema interface {
	GetType() string
	GetFields() []FieldSchema
	GetFieldNames() []string
	GetField(string) FieldSchema

	SetType(string)
	SetFields([]FieldSchema)
}
