package qdata

import "github.com/rqure/qlib/pkg/qdata"

type valueTypeProvider struct {
	ValueType qdata.ValueType
}

func (me *valueTypeProvider) Type() qdata.ValueType {
	return me.ValueType
}

func (me *valueTypeProvider) IsInt() bool {
	return me.Type() == qdata.Int
}

func (me *valueTypeProvider) IsFloat() bool {
	return me.Type() == qdata.Float
}

func (me *valueTypeProvider) IsString() bool {
	return me.Type() == qdata.String
}

func (me *valueTypeProvider) IsBool() bool {
	return me.Type() == qdata.Bool
}

func (me *valueTypeProvider) IsBinaryFile() bool {
	return me.Type() == qdata.BinaryFile
}

func (me *valueTypeProvider) IsEntityReference() bool {
	return me.Type() == qdata.EntityReference
}

func (me *valueTypeProvider) IsTimestamp() bool {
	return me.Type() == qdata.Timestamp
}

func (me *valueTypeProvider) IsChoice() bool {
	return me.Type() == qdata.Choice
}

func (me *valueTypeProvider) IsEntityList() bool {
	return me.Type() == qdata.EntityList
}
