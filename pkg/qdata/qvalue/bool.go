package qvalue

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

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
			ValueType: qdata.Bool,
		},
		ValueConstructor: me,
		AnyPbConverter:   me,
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

func (me *Bool) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Bool{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}
