package qdata

import (
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
	if v, ok := value.(string); ok {
		me.Value = v
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
