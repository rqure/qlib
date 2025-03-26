package qdata

import "slices"

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

func (me *ValueType) As(o ValueType) *ValueType {
	*me = o
	return me
}

func (me *ValueType) IsNil() bool {
	return !slices.Contains(ValueTypes, *me)
}

func (me *ValueType) IsInt() bool {
	return *me == Int
}

func (me *ValueType) IsFloat() bool {
	return *me == Float
}

func (me *ValueType) IsString() bool {
	return *me == String
}

func (me *ValueType) IsBool() bool {
	return *me == Bool
}

func (me *ValueType) IsBinaryFile() bool {
	return *me == BinaryFile
}

func (me *ValueType) IsEntityReference() bool {
	return *me == EntityReference
}

func (me *ValueType) IsTimestamp() bool {
	return *me == Timestamp
}

func (me *ValueType) IsChoice() bool {
	return *me == Choice
}

func (me *ValueType) IsEntityList() bool {
	return *me == EntityList
}
