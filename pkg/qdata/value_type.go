package qdata

import "slices"

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

func (me *ValueType) ProtobufName() string {
	switch *me {
	case VTInt:
		return "qprotobufs.Int"
	case VTFloat:
		return "qprotobufs.Float"
	case VTString:
		return "qprotobufs.String"
	case VTBool:
		return "qprotobufs.Bool"
	case VTBinaryFile:
		return "qprotobufs.BinaryFile"
	case VTEntityReference:
		return "qprotobufs.EntityReference"
	case VTTimestamp:
		return "qprotobufs.Timestamp"
	case VTChoice:
		return "qprotobufs.Choice"
	case VTEntityList:
		return "qprotobufs.EntityList"
	}

	return ""
}

func (me *ValueType) As(o ValueType) *ValueType {
	*me = o
	return me
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
