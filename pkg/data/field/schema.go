package field

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Schema struct {
	impl *protobufs.DatabaseFieldSchema
}

func prefixed(t string) string {
	return "protobufs." + t
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

	return me.impl.Type
}

func (me *Schema) IsInt() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("Int")
}

func (me *Schema) IsFloat() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("Float")
}

func (me *Schema) IsString() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("String")
}

func (me *Schema) IsBool() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("Bool")
}

func (me *Schema) IsBinaryFile() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("BinaryFile")
}

func (me *Schema) IsEntityReference() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("EntityReference")
}

func (me *Schema) IsTimestamp() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("Timestamp")
}

func (me *Schema) IsTransformation() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("Transformation")
}

func (me *Schema) IsChoice() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("Choice")
}

func (me *Schema) IsEntityList() bool {
	if me.impl == nil {
		log.Error("Impl not defined")
		return false
	}

	return me.impl.Type == prefixed("EntityList")
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
