package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Bool struct {
	Value bool
}

func NewBool(v ...bool) qdata.ModifiableValue {
	me := &Bool{
		Value: false, // Default false
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.BoolType,
		},
		RawValueProvider:    me,
		ModifiableBoolValue: me,
	}
}

func (me *Bool) GetBool() bool {
	return me.Value
}

func (me *Bool) SetBool(value bool) qdata.ModifiableBoolValue {
	me.Value = value
	return me
}

func (me *Bool) Raw() interface{} {
	return me.Value
}
