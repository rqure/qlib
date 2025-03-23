package qdata

import (
	"context"
	"time"
)

type FieldInfoProvider interface {
	GetEntityId() string
	GetFieldName() string
	GetWriteTime() time.Time
	GetWriter() string
}

type FieldValueProvider interface {
	GetValue() ValueTypeProvider
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

type FieldTypeProvider interface {
	GetFieldType() string

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

type FieldValueWriter interface {
	WriteValue(context.Context, ValueTypeProvider)
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
	ReadValue(context.Context) ValueTypeProvider
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

type Field interface {
	FieldInfoProvider
	FieldValueProvider
	FieldTypeProvider
}

type FieldSchema interface {
	GetFieldName() string

	FieldTypeProvider

	GetReadPermissions() []string
	GetWritePermissions() []string

	AsChoiceFieldSchema() ChoiceFieldSchema
}

type ChoiceFieldSchema interface {
	FieldSchema
	GetChoices() []string
	SetChoices([]string) ChoiceFieldSchema
}

type FieldValueSetter interface {
	SetValue(ValueTypeProvider)
}

type FieldBinding interface {
	FieldInfoProvider
	FieldTypeProvider

	FieldValueProvider
	FieldValueSetter

	FieldValueWriter
	FieldValueReader
}
