package data

import "time"

type MultiBinding interface {
	Store

	GetEntityById(string) EntityBinding
	Commit()
}

type EntityBinding interface {
	Entity

	GetField(string) FieldBinding
}

type FieldBinding interface {
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

	SetValue(Value) FieldBinding

	WriteValue(Value) FieldBinding
	WriteInt(...interface{}) FieldBinding
	WriteFloat(...interface{}) FieldBinding
	WriteString(...interface{}) FieldBinding
	WriteBool(...interface{}) FieldBinding
	WriteBinaryFile(...interface{}) FieldBinding
	WriteEntityReference(...interface{}) FieldBinding
	WriteTimestamp(...interface{}) FieldBinding
	WriteTransformation(...interface{}) FieldBinding

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
