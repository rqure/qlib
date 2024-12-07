package data

type Entity interface {
	GetId() string
	GetType() string
	GetName() string
	GetParentId() string
	GetChildrenIds() []string
}

type EntitySchema interface {
	GetType() string
	GetFields() []FieldSchema
}
