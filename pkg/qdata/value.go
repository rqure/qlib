package qdata

import (
	"time"

	"google.golang.org/protobuf/types/known/anypb"
)

type ValueType string

const (
	Int             ValueType = "int"
	Float           ValueType = "float"
	String          ValueType = "string"
	Bool            ValueType = "bool"
	BinaryFile      ValueType = "binaryFile"
	EntityReference ValueType = "entityReference"
	Timestamp       ValueType = "timestamp"
	Choice          ValueType = "choice"
	EntityList      ValueType = "entityList"
)

var ValueTypes = []ValueType{
	Int,
	Float,
	String,
	Bool,
	BinaryFile,
	EntityReference,
	Timestamp,
	Choice,
	EntityList,
}

func (me *ValueType) ProtobufName() string {
	switch *me {
	case Int:
		return "qprotobufs.Int"
	case Float:
		return "qprotobufs.Float"
	case String:
		return "qprotobufs.String"
	case Bool:
		return "qprotobufs.Bool"
	case BinaryFile:
		return "qprotobufs.BinaryFile"
	case EntityReference:
		return "qprotobufs.EntityReference"
	case Timestamp:
		return "qprotobufs.Timestamp"
	case Choice:
		return "qprotobufs.Choice"
	case EntityList:
		return "qprotobufs.EntityList"
	}

	return ""
}

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

func (me *Value) Update(o *Value) {
	me.ValueConstructor = o.ValueConstructor
	me.AnyPbConverter = o.AnyPbConverter
	me.ValueTypeProvider = o.ValueTypeProvider
	me.RawProvider = o.RawProvider
	me.IntProvider = o.IntProvider
	me.FloatProvider = o.FloatProvider
	me.StringProvider = o.StringProvider
	me.BoolProvider = o.BoolProvider
	me.BinaryFileProvider = o.BinaryFileProvider
	me.EntityReferenceProvider = o.EntityReferenceProvider
	me.TimestampProvider = o.TimestampProvider
	me.ChoiceProvider = o.ChoiceProvider
	me.EntityListProvider = o.EntityListProvider
	me.RawReceiver = o.RawReceiver
	me.IntReceiver = o.IntReceiver
	me.FloatReceiver = o.FloatReceiver
	me.StringReceiver = o.StringReceiver
	me.BoolReceiver = o.BoolReceiver
	me.BinaryFileReceiver = o.BinaryFileReceiver
	me.EntityReferenceReceiver = o.EntityReferenceReceiver
	me.TimestampReceiver = o.TimestampReceiver
	me.ChoiceReceiver = o.ChoiceReceiver
	me.EntityListReceiver = o.EntityListReceiver
}
