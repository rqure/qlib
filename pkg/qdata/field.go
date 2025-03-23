package qdata

import (
	"time"
)

type EntityTypeProvider interface {
	GetEntityType() string
}

type EntityIdProvider interface {
	GetEntityId() string
}

type FieldNameProvider interface {
	GetFieldName() string
}

type WriteInfoProvider interface {
	GetWriteTime() time.Time
	GetWriter() string
}

type PermissionProvider interface {
	GetReadPermissions() []string
	GetWritePermissions() []string
}

type Field interface {
	EntityIdProvider
	FieldNameProvider
	ValueProvider
	WriteInfoProvider
}

type FieldSchema interface {
	EntityTypeProvider
	FieldNameProvider
	ValueTypeProvider
	PermissionProvider
}
