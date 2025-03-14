package qdata

import (
	"time"
)

type Field interface {
	GetValue() Value
	GetWriteTime() time.Time
	GetWriter() string
	GetEntityId() string
	GetFieldName() string
}

type Value interface {
	IsNil() bool
	IsInt() bool
	IsFloat() bool
	IsString() bool
	IsBool() bool
	IsBinaryFile() bool
	IsEntityReference() bool
	IsTimestamp() bool
	IsChoice() bool
	IsEntityList() bool

	GetType() string
	GetInt() int64
	GetFloat() float64
	GetString() string
	GetBool() bool
	GetBinaryFile() string
	GetEntityReference() string
	GetTimestamp() time.Time
	GetChoice() Choice
	GetEntityList() EntityList

	SetInt(interface{}) Value
	SetFloat(interface{}) Value
	SetString(interface{}) Value
	SetBool(interface{}) Value
	SetBinaryFile(interface{}) Value
	SetEntityReference(interface{}) Value
	SetTimestamp(interface{}) Value
	SetChoice(interface{}) Value
	SetEntityList(interface{}) Value
}

type FieldSchema interface {
	GetFieldName() string
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

	GetReadPermissions() []string
	GetWritePermissions() []string

	AsChoiceFieldSchema() ChoiceFieldSchema
}

type ChoiceFieldSchema interface {
	FieldSchema
	GetChoices() []string
	SetChoices([]string) ChoiceFieldSchema
}
