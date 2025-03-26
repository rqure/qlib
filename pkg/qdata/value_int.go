package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueInt struct {
	Value int
}

func NewInt(v ...int) *Value {
	me := &ValueInt{
		Value: 0,
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: new(ValueType).As(Int),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		RawProvider:       me,
		RawReceiver:       me,
		IntProvider:       me,
		IntReceiver:       me,
	}
}

func (me *ValueInt) GetInt() int {
	return me.Value
}

func (me *ValueInt) SetInt(value int) {
	me.Value = value
}

func (me *ValueInt) GetRaw() interface{} {
	return me.Value
}

func (me *ValueInt) SetRaw(value interface{}) {
	if v, ok := value.(int); ok {
		me.Value = v
	}
}

func (me *ValueInt) Clone() *Value {
	return NewInt(me.Value)
}

func (me *ValueInt) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Int{
		Raw: int64(me.Value),
	})

	if err != nil {
		return nil
	}

	return a
}
