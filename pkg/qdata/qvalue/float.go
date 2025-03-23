package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Float struct {
	Value float64
}

func NewFloat(v ...float64) qdata.ModifiableValue {
	me := &Float{
		Value: 0.0,
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.FloatType,
		},
		RawValueProvider:     me,
		ModifiableFloatValue: me,
	}
}

func (me *Float) GetFloat() float64 {
	return me.Value
}

func (me *Float) SetFloat(value float64) qdata.ModifiableFloatValue {
	me.Value = value
	return me
}

func (me *Float) Raw() interface{} {
	return me.Value
}
