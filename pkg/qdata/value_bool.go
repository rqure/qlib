package qdata

import (
	"github.com/rqure/qlib/pkg/qlog"
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
		ValueTypeProvider: new(ValueType).As(VTBool),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		StringConverter:   me,
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

func (me *ValueBool) GetRaw() any {
	return me.Value
}

func (me *ValueBool) SetRaw(value any) {
	switch v := value.(type) {
	case bool:
		me.Value = v
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
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

func (me *ValueBool) AsString() string {
	if me.Value {
		return "true"
	}
	return "false"
}
