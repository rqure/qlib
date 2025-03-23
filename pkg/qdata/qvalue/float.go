package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Float struct {
	Value float64
}

func NewFloat(v ...float64) qdata.Value {
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
		RawProvider:   me,
		RawReceiver:   me,
		FloatProvider: me,
		FloatReceiver: me,
	}
}

func (me *Float) GetFloat() float64 {
	return me.Value
}

func (me *Float) SetFloat(value float64) {
	me.Value = value
}

func (me *Float) GetRaw() interface{} {
	return me.Value
}

func (me *Float) SetRaw(value interface{}) {
	if v, ok := value.(float64); ok {
		me.Value = v
	}
}
