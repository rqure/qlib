package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type Choice struct {
	Value int
}

func NewChoice(v ...int) qdata.ModifiableValue {
	me := &Choice{
		Value: 0, // Default choice index
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.ChoiceType,
		},
		RawValueProvider:      me,
		ModifiableChoiceValue: me,
	}
}

func (me *Choice) GetChoice() int {
	return me.Value
}

func (me *Choice) SetChoice(value int) qdata.ModifiableChoiceValue {
	me.Value = value
	return me
}

func (me *Choice) Raw() interface{} {
	return me.Value
}
