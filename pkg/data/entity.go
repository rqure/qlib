package data

const (
	EntityTypeRoot       = "Root"
	EntityTypePermission = "Permission"
	EntityTypeUser       = "User"
	EntityTypeClient     = "Client"
	EntityTypeRole       = "Role"
)

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
