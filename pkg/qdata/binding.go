package qdata

import (
	"context"
	"time"
)

type EntityBinding interface {
	Entity
	GetField(string) FieldBinding
}

type FieldBasicInfo interface {
	GetEntityId() string
	GetFieldName() string
	GetWriteTime() time.Time
	GetWriter() string
}

type FieldTypeInfo interface {
	IsInt() bool
	IsFloat() bool
	IsString() bool
	IsBool() bool
	IsBinaryFile() bool
	IsEntityReference() bool
	IsTimestamp() bool
	IsChoice() bool
	IsEntityList() bool
}

type FieldValueGetter interface {
	GetValue() Value
	GetInt() int64
	GetFloat() float64
	GetString() string
	GetBool() bool
	GetBinaryFile() string
	GetEntityReference() string
	GetTimestamp() time.Time
	GetChoice() Choice
	GetCompleteChoice(context.Context) CompleteChoice
	GetEntityList() EntityList
}

type FieldValueSetter interface {
	SetValue(Value)
}

type FieldValueWriter interface {
	WriteValue(context.Context, Value)
	WriteInt(context.Context, ...interface{})
	WriteFloat(context.Context, ...interface{})
	WriteString(context.Context, ...interface{})
	WriteBool(context.Context, ...interface{})
	WriteBinaryFile(context.Context, ...interface{})
	WriteEntityReference(context.Context, ...interface{})
	WriteTimestamp(context.Context, ...interface{})
	WriteChoice(context.Context, ...interface{})
	WriteEntityList(context.Context, ...interface{})
}

type FieldValueReader interface {
	ReadValue(context.Context) Value
	ReadInt(context.Context) int64
	ReadFloat(context.Context) float64
	ReadString(context.Context) string
	ReadBool(context.Context) bool
	ReadBinaryFile(context.Context) string
	ReadEntityReference(context.Context) string
	ReadTimestamp(context.Context) time.Time
	ReadChoice(context.Context) CompleteChoice
	ReadEntityList(context.Context) EntityList
}

type FieldBinding interface {
	FieldBasicInfo
	FieldTypeInfo

	FieldValueGetter
	FieldValueSetter

	FieldValueWriter
	FieldValueReader
}
