package qfield

import (
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

const (
	Int             = "Int"
	Float           = "Float"
	String          = "String"
	Bool            = "Bool"
	BinaryFile      = "BinaryFile"
	EntityReference = "EntityReference"
	Timestamp       = "Timestamp"
	Choice          = "Choice"
	EntityList      = "EntityList"
)

type Schema struct {
	impl *qprotobufs.DatabaseFieldSchema
}

func PrefixedType(t string) string {
	if strings.HasPrefix(t, "qprotobufs.") {
		return t
	}

	return "qprotobufs." + t
}

func UnprefixedType(t string) string {
	return strings.Replace(t, "qprotobufs.", "", 1)
}

func Types() []string {
	return []string{
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
}

func NewSchema(name, fieldType string) qdata.FieldSchema {
	return &Schema{
		impl: &qprotobufs.DatabaseFieldSchema{
			Name: name,
			Type: PrefixedType(fieldType),
		},
	}
}

func ToSchemaPb(s qdata.FieldSchema) *qprotobufs.DatabaseFieldSchema {
	if s == nil {
		return nil
	}

	switch c := s.(type) {
	case *Schema:
		return c.impl
	default:
		return nil
	}
}

func FromSchemaPb(impl *qprotobufs.DatabaseFieldSchema) qdata.FieldSchema {
	return &Schema{
		impl: impl,
	}
}

func (me *Schema) GetFieldName() string {
	if me.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return me.impl.Name
}

func (me *Schema) GetFieldType() string {
	if me.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return UnprefixedType(me.impl.Type)
}

func (me *Schema) IsInt() bool {
	return me.GetFieldType() == "Int"
}

func (me *Schema) IsFloat() bool {
	return me.GetFieldType() == "Float"
}

func (me *Schema) IsString() bool {
	return me.GetFieldType() == "String"
}

func (me *Schema) IsBool() bool {
	return me.GetFieldType() == "Bool"
}

func (me *Schema) IsBinaryFile() bool {
	return me.GetFieldType() == "BinaryFile"
}

func (me *Schema) IsEntityReference() bool {
	return me.GetFieldType() == "EntityReference"
}

func (me *Schema) IsTimestamp() bool {
	return me.GetFieldType() == "Timestamp"
}

func (me *Schema) IsChoice() bool {
	return me.GetFieldType() == "Choice"
}

func (me *Schema) IsEntityList() bool {
	return me.GetFieldType() == "EntityList"
}

func (me *Schema) GetReadPermissions() []string {
	if me.impl == nil {
		qlog.Error("Impl not defined")
		return []string{}
	}

	result := make([]string, len(me.impl.ReadPermissions))
	copy(result, me.impl.ReadPermissions)
	return result
}

func (me *Schema) GetWritePermissions() []string {
	if me.impl == nil {
		qlog.Error("Impl not defined")
		return []string{}
	}

	result := make([]string, len(me.impl.WritePermissions))
	copy(result, me.impl.WritePermissions)
	return result
}

func (me *Schema) AsChoiceFieldSchema() qdata.ChoiceFieldSchema {
	if me.IsChoice() {
		return me
	}

	return nil
}

func (me *Schema) GetChoices() []string {
	if me.impl == nil {
		qlog.Error("Impl not defined")
		return []string{}
	}

	result := make([]string, len(me.impl.ChoiceOptions))
	copy(result, me.impl.ChoiceOptions)
	return result
}

func (me *Schema) SetChoices(options []string) qdata.ChoiceFieldSchema {
	if me.impl == nil {
		qlog.Error("Impl not defined")
		return nil
	}

	me.impl.ChoiceOptions = options
	return me
}
