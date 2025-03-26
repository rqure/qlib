package qdata

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueChoice struct {
	Value int
}

func NewChoice(v ...int) *qdata.Value {
	me := &ValueChoice{
		Value: 0, // Default choice index
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &valueTypeProvider{
			ValueType: qdata.Choice,
		},
		ValueConstructor: me,
		AnyPbConverter:   me,
		RawProvider:      me,
		RawReceiver:      me,
		ChoiceProvider:   me,
		ChoiceReceiver:   me,
	}
}

func (me *ValueChoice) GetChoice() int {
	return me.Value
}

func (me *ValueChoice) SetChoice(value int) {
	me.Value = value
}

func (me *ValueChoice) GetRaw() interface{} {
	return me.Value
}

func (me *ValueChoice) SetRaw(value interface{}) {
	if v, ok := value.(int); ok {
		me.Value = v
	}
}

func (me *ValueChoice) Clone() *qdata.Value {
	return NewChoice(me.Value)
}

func (me *ValueChoice) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Choice{
		Raw: int64(me.Value),
	})

	if err != nil {
		return nil
	}

	return a
}
