package qdata

import (
	"slices"

	"github.com/rqure/qlib/pkg/qlog"
)

type ValueType string

const (
	VTInt             ValueType = "int"
	VTFloat           ValueType = "float"
	VTString          ValueType = "string"
	VTBool            ValueType = "bool"
	VTBinaryFile      ValueType = "binaryFile"
	VTEntityReference ValueType = "entityReference"
	VTTimestamp       ValueType = "timestamp"
	VTChoice          ValueType = "choice"
	VTEntityList      ValueType = "entityList"
)

var ValueTypes = []ValueType{
	VTInt,
	VTFloat,
	VTString,
	VTBool,
	VTBinaryFile,
	VTEntityReference,
	VTTimestamp,
	VTChoice,
	VTEntityList,
}

type ValueTypeProvider interface {
	Type() ValueType

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
}

func (me ValueType) AsString() string {
	return string(me)
}

func (me *ValueType) As(o ValueType) *ValueType {
	*me = o
	return me
}

func (me *ValueType) Type() ValueType {
	return *me
}

func (me *ValueType) IsNil() bool {
	return !slices.Contains(ValueTypes, *me)
}

func (me *ValueType) IsInt() bool {
	return *me == VTInt
}

func (me *ValueType) IsFloat() bool {
	return *me == VTFloat
}

func (me *ValueType) IsString() bool {
	return *me == VTString
}

func (me *ValueType) IsBool() bool {
	return *me == VTBool
}

func (me *ValueType) IsBinaryFile() bool {
	return *me == VTBinaryFile
}

func (me *ValueType) IsEntityReference() bool {
	return *me == VTEntityReference
}

func (me *ValueType) IsTimestamp() bool {
	return *me == VTTimestamp
}

func (me *ValueType) IsChoice() bool {
	return *me == VTChoice
}

func (me *ValueType) IsEntityList() bool {
	return *me == VTEntityList
}

func (me ValueType) NewValue(args ...any) *Value {
	var value *Value

	switch me {
	case VTInt:
		value = NewInt()
	case VTFloat:
		value = NewFloat()
	case VTString:
		value = NewString()
	case VTBool:
		value = NewBool()
	case VTBinaryFile:
		value = NewBinaryFile()
	case VTEntityReference:
		value = NewEntityReference()
	case VTTimestamp:
		value = NewTimestamp()
	case VTChoice:
		value = NewChoice()
	case VTEntityList:
		value = NewEntityList()
	default:
		qlog.Error("Invalid value type: '%s'", me)
	}

	if value != nil && len(args) > 0 {
		value.SetRaw(args[0])
	}

	return value
}
