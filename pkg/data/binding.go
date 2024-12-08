package data

import "time"

type WriteOpt int

const (
	WriteNormal WriteOpt = iota
	WriteChanges
)

type Binding interface {
	GetEntityId() string
	GetFieldName() string
	GetWriteTime() time.Time
	GetWriter() string

	IsInt() bool
	IsFloat() bool
	IsString() bool
	IsBool() bool
	IsBinaryFile() bool
	IsEntityReference() bool
	IsTimestamp() bool
	IsTransformation() bool

	GetValue() Value
	GetInt() int64
	GetFloat() float64
	GetString() string
	GetBool() bool
	GetBinaryFile() string
	GetEntityReference() string
	GetTimestamp() time.Time
	GetTransformation() string

	WriteValue(Value) Binding
	WriteInt(...interface{}) Binding
	WriteFloat(...interface{}) Binding
	WriteString(...interface{}) Binding
	WriteBool(...interface{}) Binding
	WriteBinaryFile(...interface{}) Binding
	WriteEntityReference(...interface{}) Binding
	WriteTimestamp(...interface{}) Binding
	WriteTransformation(...interface{}) Binding

	ReadValue() Value
	ReadInt() int64
	ReadFloat() float64
	ReadString() string
	ReadBool() bool
	ReadBinaryFile() string
	ReadEntityReference() string
	ReadTimestamp() time.Time
	ReadTransformation() string
}
