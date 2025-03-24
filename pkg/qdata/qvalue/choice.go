package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Choice struct {
	Value int
}

func NewChoice(v ...int) *qdata.Value {
	me := &Choice{
		Value: 0, // Default choice index
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.ChoiceType,
		},
		ValueConstructor: me,
		RawProvider:      me,
		RawReceiver:      me,
		ChoiceProvider:   me,
		ChoiceReceiver:   me,
	}
}

func (me *Choice) GetChoice() int {
	return me.Value
}

func (me *Choice) SetChoice(value int) {
	me.Value = value
}

func (me *Choice) GetRaw() interface{} {
	return me.Value
}

func (me *Choice) SetRaw(value interface{}) {
	if v, ok := value.(int); ok {
		me.Value = v
	}
}

func (me *Choice) Clone() *qdata.Value {
	return NewChoice(me.Value)
}
