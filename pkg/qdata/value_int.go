package qdata

import (
	"fmt"
	"strconv"

	"github.com/rqure/qlib/pkg/qlog"
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
		ValueTypeProvider: new(ValueType).As(VTInt),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		StringConverter:   me,
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

func (me *ValueInt) GetRaw() any {
	return me.Value
}

func (me *ValueInt) SetRaw(value any) {
	switch v := value.(type) {
	case int:
		me.Value = v
	case int8:
		me.Value = int(v)
	case int16:
		me.Value = int(v)
	case int32:
		me.Value = int(v)
	case int64:
		me.Value = int(v)
	case uint:
		me.Value = int(v)
	case uint8:
		me.Value = int(v)
	case uint16:
		me.Value = int(v)
	case uint32:
		me.Value = int(v)
	case uint64:
		me.Value = int(v)
	case float32:
		me.Value = int(v)
	case float64:
		me.Value = int(v)
	case string:
		var err error
		me.Value, err = strconv.Atoi(v)
		if err != nil {
			qlog.Error("Failed to convert string to int: %v", err)
			return
		}
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
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

func (me *ValueInt) AsString() string {
	return fmt.Sprintf("%d", me.Value)
}
