package qdata

import (
	"time"

	"google.golang.org/protobuf/types/known/anypb"
)

type ValueType string

const (
	IntType             ValueType = "int"
	FloatType           ValueType = "float"
	StringType          ValueType = "string"
	BoolType            ValueType = "bool"
	BinaryFileType      ValueType = "binaryFile"
	EntityReferenceType ValueType = "entityReference"
	TimestampType       ValueType = "timestamp"
	ChoiceType          ValueType = "choice"
	EntityListType      ValueType = "entityList"
)

type ValueTypeProvider interface {
	Type() ValueType
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

type ValueTypeReceiver interface {
	SetType(value ValueType)
}

type RawProvider interface {
	GetRaw() interface{}
}

type RawReceiver interface {
	SetRaw(value interface{})
}

type IntProvider interface {
	GetInt() int
}

type FloatProvider interface {
	GetFloat() float64
}

type StringProvider interface {
	GetString() string
}

type BoolProvider interface {
	GetBool() bool
}

type BinaryFileProvider interface {
	GetBinaryFile() string
}

type EntityReferenceProvider interface {
	GetEntityReference() string
}

type TimestampProvider interface {
	GetTimestamp() time.Time
}

type ChoiceProvider interface {
	GetChoice() int
}

type EntityListProvider interface {
	GetEntityList() []string
}

type IntReceiver interface {
	SetInt(value int)
}

type FloatReceiver interface {
	SetFloat(value float64)
}

type StringReceiver interface {
	SetString(value string)
}

type BoolReceiver interface {
	SetBool(value bool)
}

type BinaryFileReceiver interface {
	SetBinaryFile(value string)
}

type EntityReferenceReceiver interface {
	SetEntityReference(value string)
}

type TimestampReceiver interface {
	SetTimestamp(value time.Time)
}

type ChoiceReceiver interface {
	SetChoice(value int)
}

type EntityListReceiver interface {
	SetEntityList(value []string)
}

type ValueConstructor interface {
	Clone() *Value
}

type AnyPbConverter interface {
	AsAnyPb() *anypb.Any
}

type Value struct {
	ValueConstructor
	AnyPbConverter
	ValueTypeProvider

	RawProvider
	IntProvider
	FloatProvider
	StringProvider
	BoolProvider
	BinaryFileProvider
	EntityReferenceProvider
	TimestampProvider
	ChoiceProvider
	EntityListProvider

	RawReceiver
	IntReceiver
	FloatReceiver
	StringReceiver
	BoolReceiver
	BinaryFileReceiver
	EntityReferenceReceiver
	TimestampReceiver
	ChoiceReceiver
	EntityListReceiver
}
