package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type BinaryFile struct {
	Value string
}

func NewBinaryFile(v ...string) qdata.Value {
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
		RawProvider:        me,
		RawReceiver:        me,
		BinaryFileProvider: me,
		BinaryFileReceiver: me,
	}
}

func (me *BinaryFile) GetBinaryFile() string {
	return me.Value
}

func (me *BinaryFile) SetBinaryFile(value string) {
	me.Value = value
}

func (me *BinaryFile) GetRaw() interface{} {
	return me.Value
}

func (me *BinaryFile) SetRaw(value interface{}) {
	if v, ok := value.(string); ok {
		me.Value = v
	}
}
