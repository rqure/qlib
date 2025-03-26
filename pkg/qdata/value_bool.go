package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueBool struct {
	Value bool
}

func NewBool(v ...bool) *Value {
	me := &ValueBool{
		Value: false, // Default false
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: new(ValueType).As(Bool),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		RawProvider:       me,
		RawReceiver:       me,
		BoolProvider:      me,
		BoolReceiver:      me,
	}
}

func (me *ValueBool) GetBool() bool {
	return me.Value
}

func (me *ValueBool) SetBool(value bool) {
	me.Value = value
}

func (me *ValueBool) GetRaw() interface{} {
	return me.Value
}

func (me *ValueBool) SetRaw(value interface{}) {
	if v, ok := value.(bool); ok {
		me.Value = v
	}
}

func (me *ValueBool) Clone() *Value {
	return NewBool(me.Value)
}

func (me *ValueBool) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Bool{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}
