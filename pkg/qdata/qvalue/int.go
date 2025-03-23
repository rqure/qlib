package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Int struct {
	Value int
}

func NewInt(v ...int) qdata.ModifiableValue {
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
		RawValueProvider:   me,
		ModifiableIntValue: me,
	}
}

func (me *Int) GetInt() int {
	return me.Value
}

func (me *Int) SetInt(value int) qdata.ModifiableIntValue {
	me.Value = value
	return me
}

func (me *Int) Raw() interface{} {
	return me.Value
}
