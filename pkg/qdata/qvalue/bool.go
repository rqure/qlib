package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Bool struct {
	Value bool
}

func NewBool(v ...bool) *qdata.Value {
	me := &Bool{
		Value: false, // Default false
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.BoolType,
		},
		ValueConstructor: me,
		RawProvider:      me,
		RawReceiver:      me,
		BoolProvider:     me,
		BoolReceiver:     me,
	}
}

func (me *Bool) GetBool() bool {
	return me.Value
}

func (me *Bool) SetBool(value bool) {
	me.Value = value
}

func (me *Bool) GetRaw() interface{} {
	return me.Value
}

func (me *Bool) SetRaw(value interface{}) {
	if v, ok := value.(bool); ok {
		me.Value = v
	}
}

func (me *Bool) Clone() *qdata.Value {
	return NewBool(me.Value)
}
