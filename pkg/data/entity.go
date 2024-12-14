package data

type Entity interface {
	GetId() string
	GetType() string
	GetName() string
	GetParentId() string
	GetChildrenIds() []string

	AppendChildId(string)
	RemoveChildId(string)
	SetChildrenIds([]string)

	SetId(string)
	SetType(string)
	SetName(string)
	SetParentId(string)

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
