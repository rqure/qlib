package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type ValueTypeProvider struct {
	ValueType qdata.ValueType
}

func (me *ValueTypeProvider) Type() qdata.ValueType {
	return me.ValueType
}

func (me *ValueTypeProvider) IsInt() bool {
	return me.Type() == qdata.Int
}

func (me *ValueTypeProvider) IsFloat() bool {
	return me.Type() == qdata.Float
}

func (me *ValueTypeProvider) IsString() bool {
	return me.Type() == qdata.String
}

func (me *ValueTypeProvider) IsBool() bool {
	return me.Type() == qdata.Bool
}

func (me *ValueTypeProvider) IsBinaryFile() bool {
	return me.Type() == qdata.BinaryFile
}

func (me *ValueTypeProvider) IsEntityReference() bool {
	return me.Type() == qdata.EntityReference
}

func (me *ValueTypeProvider) IsTimestamp() bool {
	return me.Type() == qdata.Timestamp
}

func (me *ValueTypeProvider) IsChoice() bool {
	return me.Type() == qdata.Choice
}

func (me *ValueTypeProvider) IsEntityList() bool {
	return me.Type() == qdata.EntityList
}
