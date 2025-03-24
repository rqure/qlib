package qvalue

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type Float struct {
	Value float64
}

func NewFloat(v ...float64) *qdata.Value {
	me := &Float{
		Value: 0.0,
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.FloatType,
		},
		ValueConstructor: me,
		AnyPbConverter:   me,
		RawProvider:      me,
		RawReceiver:      me,
		FloatProvider:    me,
		FloatReceiver:    me,
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

func (me *Float) Clone() *qdata.Value {
	return NewFloat(me.Value)
}

func (me *Float) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Float{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}
