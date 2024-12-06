package data

import (
	"time"
)

type Field interface {
	GetInt() int64
	GetFloat() float64
	GetString() string
	GetBool() bool
	GetBinaryFile() string
	GetEntityReference() string
	GetTimestamp() time.Time
	GetTransformation() string
	GetWriteTime() time.Time
	GetWriter() string
	GetEntityId() string
	GetEntityName() string
}
