package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Int struct {
	Value int
}

func NewInt(v ...int) qdata.Value {
	me := &Int{
		Value: 0,
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.IntType,
		},
		RawProvider: me,
		RawReceiver: me,
		IntProvider: me,
		IntReceiver: me,
	}
}

func (me *Int) GetInt() int {
	return me.Value
}

func (me *Int) SetInt(value int) {
	me.Value = value
}

func (me *Int) GetRaw() interface{} {
	return me.Value
}

func (me *Int) SetRaw(value interface{}) {
	if v, ok := value.(int); ok {
		me.Value = v
	}
}
