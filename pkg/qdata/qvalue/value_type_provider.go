package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type ValueTypeProvider struct {
	ValueType qdata.ValueType
}

func (me *ValueTypeProvider) Type() qdata.ValueType {
	return me.ValueType
}

func (me *ValueTypeProvider) IsInt() bool {
	return me.Type() == qdata.IntType
}

func (me *ValueTypeProvider) IsFloat() bool {
	return me.Type() == qdata.FloatType
}

func (me *ValueTypeProvider) IsString() bool {
	return me.Type() == qdata.StringType
}

func (me *ValueTypeProvider) IsBool() bool {
	return me.Type() == qdata.BoolType
}

func (me *ValueTypeProvider) IsBinaryFile() bool {
	return me.Type() == qdata.BinaryFileType
}

func (me *ValueTypeProvider) IsEntityReference() bool {
	return me.Type() == qdata.EntityReferenceType
}

func (me *ValueTypeProvider) IsTimestamp() bool {
	return me.Type() == qdata.TimestampType
}

func (me *ValueTypeProvider) IsChoice() bool {
	return me.Type() == qdata.ChoiceType
}

func (me *ValueTypeProvider) IsEntityList() bool {
	return me.Type() == qdata.EntityListType
}
