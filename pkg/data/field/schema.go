package field

import (
	"strings"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
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
	impl *protobufs.DatabaseFieldSchema
}

func PrefixedType(t string) string {
	if strings.HasPrefix(t, "protobufs.") {
		return t
	}

	return "protobufs." + t
}

func UnprefixedType(t string) string {
	return strings.Replace(t, "protobufs.", "", 1)
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

func NewSchema(name, fieldType string) data.FieldSchema {
	return &Schema{
		impl: &protobufs.DatabaseFieldSchema{
			Name: name,
			Type: PrefixedType(fieldType),
		},
	}
}

func ToSchemaPb(s data.FieldSchema) *protobufs.DatabaseFieldSchema {
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

func FromSchemaPb(impl *protobufs.DatabaseFieldSchema) data.FieldSchema {
	return &Schema{
		impl: impl,
	}
}

func (me *Schema) GetFieldName() string {
	if me.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return me.impl.Name
}

func (me *Schema) GetFieldType() string {
	if me.impl == nil {
		log.Error("Impl not defined")
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
		log.Error("Impl not defined")
		return []string{}
	}

	return me.impl.ReadPermissions
}

func (me *Schema) GetWritePermissions() []string {
	if me.impl == nil {
		log.Error("Impl not defined")
		return []string{}
	}

	return me.impl.WritePermissions
}

func (me *Schema) AsChoiceFieldSchema() data.ChoiceFieldSchema {
	if me.IsChoice() {
		return me
	}

	return nil
}

func (me *Schema) GetChoices() []string {
	if me.impl == nil {
		log.Error("Impl not defined")
		return []string{}
	}

	return me.impl.ChoiceOptions
}

func (me *Schema) SetChoices(options []string) data.ChoiceFieldSchema {
	if me.impl == nil {
		log.Error("Impl not defined")
		return nil
	}

	me.impl.ChoiceOptions = options
	return me
}
