package qdata

import "time"

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

type RawValueProvider interface {
	Raw() interface{}
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

type Value interface {
	RawValueProvider
	ValueTypeProvider

	IntValue
	FloatValue
	StringValue
	BoolValue
	BinaryFileValue
	EntityReferenceValue
	TimestampValue
	ChoiceValue
	EntityListValue
}

type ModifiableValue interface {
	RawValueProvider
	ValueTypeProvider

	ModifiableIntValue
	ModifiableFloatValue
	ModifiableStringValue
	ModifiableBoolValue
	ModifiableBinaryFileValue
	ModifiableEntityReferenceValue
	ModifiableTimestampValue
	ModifiableChoiceValue
	ModifiableEntityListValue
}

type IntValue interface {
	GetInt() int
}

type FloatValue interface {
	GetFloat() float64
}

type StringValue interface {
	GetString() string
}

type BoolValue interface {
	GetBool() bool
}

type BinaryFileValue interface {
	GetBinaryFile() string
}

type EntityReferenceValue interface {
	GetEntityReference() string
}

type TimestampValue interface {
	GetTimestamp() time.Time
}

type ChoiceValue interface {
	GetChoice() int
}

type EntityListValue interface {
	GetEntityList() []string
}

type ModifiableIntValue interface {
	IntValue
	SetInt(int) ModifiableIntValue
}

type ModifiableFloatValue interface {
	FloatValue
	SetFloat(float64) ModifiableFloatValue
}

type ModifiableStringValue interface {
	StringValue
	SetString(string) ModifiableStringValue
}

type ModifiableBoolValue interface {
	BoolValue
	SetBool(bool) ModifiableBoolValue
}

type ModifiableBinaryFileValue interface {
	BinaryFileValue
	SetBinaryFile(string) ModifiableBinaryFileValue
}

type ModifiableEntityReferenceValue interface {
	EntityReferenceValue
	SetEntityReference(string) ModifiableEntityReferenceValue
}

type ModifiableTimestampValue interface {
	TimestampValue
	SetTimestamp(time.Time) ModifiableTimestampValue
}

type ModifiableChoiceValue interface {
	ChoiceValue
	SetChoice(int) ModifiableChoiceValue
}

type ModifiableEntityListValue interface {
	EntityListValue
	SetEntityList([]string) ModifiableEntityListValue
}
