package data

import (
	"context"
	"time"
)

type MultiBinding interface {
	Store

	GetEntityById(context.Context, string) EntityBinding
	Commit(context.Context)
}

type EntityBinding interface {
	Entity

	DoMulti(context.Context, func(EntityBinding))

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

	WriteValue(context.Context, Value) FieldBinding
	WriteInt(context.Context, ...interface{}) FieldBinding
	WriteFloat(context.Context, ...interface{}) FieldBinding
	WriteString(context.Context, ...interface{}) FieldBinding
	WriteBool(context.Context, ...interface{}) FieldBinding
	WriteBinaryFile(context.Context, ...interface{}) FieldBinding
	WriteEntityReference(context.Context, ...interface{}) FieldBinding
	WriteTimestamp(context.Context, ...interface{}) FieldBinding
	WriteTransformation(context.Context, ...interface{}) FieldBinding

	ReadValue(context.Context) Value
	ReadInt(context.Context) int64
	ReadFloat(context.Context) float64
	ReadString(context.Context) string
	ReadBool(context.Context) bool
	ReadBinaryFile(context.Context) string
	ReadEntityReference(context.Context) string
	ReadTimestamp(context.Context) time.Time
	ReadTransformation(context.Context) string
}
