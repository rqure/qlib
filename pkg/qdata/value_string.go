package qdata

import (
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueString struct {
	Value string
}

func NewString(v ...string) *Value {
	me := &ValueString{
		Value: "",
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: new(ValueType).As(VTString),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		StringConverter:   me,
		RawProvider:       me,
		RawReceiver:       me,
		StringProvider:    me,
		StringReceiver:    me,
	}
}

func (me *ValueString) GetString() string {
	return me.Value
}

func (me *ValueString) SetString(value string) {
	me.Value = value
}

func (me *ValueString) GetRaw() interface{} {
	return me.Value
}

func (me *ValueString) SetRaw(value interface{}) {
	switch v := value.(type) {
	case string:
		me.Value = v
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
	}
}

func (me *ValueString) Clone() *Value {
	return NewString(me.Value)
}

func (me *ValueString) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.String{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}

func (me *ValueString) AsString() string {
	return me.Value
}
