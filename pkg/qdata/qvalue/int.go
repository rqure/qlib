package qvalue

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type Int struct {
	Value int
}

func NewInt(v ...int) *qdata.Value {
	me := &Int{
		Value: 0,
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.IntType,
		},
		ValueConstructor: me,
		AnyPbConverter:   me,
		RawProvider:      me,
		RawReceiver:      me,
		IntProvider:      me,
		IntReceiver:      me,
	}
}

func (me *Int) GetInt() int {
	return me.Value
}

func (me *Int) SetInt(value int) {
	me.Value = value
}

func (me *Int) GetRaw() interface{} {
	return me.Value
}

func (me *Int) SetRaw(value interface{}) {
	if v, ok := value.(int); ok {
		me.Value = v
	}
}

func (me *Int) Clone() *qdata.Value {
	return NewInt(me.Value)
}

func (me *Int) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Int{
		Raw: int64(me.Value),
	})

	if err != nil {
		return nil
	}

	return a
}
