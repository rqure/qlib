package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueFloat struct {
	Value float64
}

func NewFloat(v ...float64) *Value {
	me := &ValueFloat{
		Value: 0.0,
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: new(ValueType).As(Float),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		RawProvider:       me,
		RawReceiver:       me,
		FloatProvider:     me,
		FloatReceiver:     me,
	}
}

func (me *ValueFloat) GetFloat() float64 {
	return me.Value
}

func (me *ValueFloat) SetFloat(value float64) {
	me.Value = value
}

func (me *ValueFloat) GetRaw() interface{} {
	return me.Value
}

func (me *ValueFloat) SetRaw(value interface{}) {
	if v, ok := value.(float64); ok {
		me.Value = v
	}
}

func (me *ValueFloat) Clone() *Value {
	return NewFloat(me.Value)
}

func (me *ValueFloat) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Float{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}
