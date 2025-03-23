package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type BinaryFile struct {
	Value string
}

func NewBinaryFile(v ...string) qdata.ModifiableValue {
	me := &BinaryFile{
		Value: "", // Empty path as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.BinaryFileType,
		},
		RawValueProvider:          me,
		ModifiableBinaryFileValue: me,
	}
}

func (me *BinaryFile) GetBinaryFile() string {
	return me.Value
}

func (me *BinaryFile) SetBinaryFile(value string) qdata.ModifiableBinaryFileValue {
	me.Value = value
	return me
}

func (me *BinaryFile) Raw() interface{} {
	return me.Value
}
