package qdata

import (
	"fmt"
	"strconv"

	"github.com/rqure/qlib/pkg/qlog"
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
		ValueTypeProvider: new(ValueType).As(VTFloat),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		StringConverter:   me,
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

func (me *ValueFloat) GetRaw() any {
	return me.Value
}

func (me *ValueFloat) SetRaw(value any) {
	switch v := value.(type) {
	case float32:
		me.Value = float64(v)
	case float64:
		me.Value = v
	case int:
		me.Value = float64(v)
	case int8:
		me.Value = float64(v)
	case int16:
		me.Value = float64(v)
	case int32:
		me.Value = float64(v)
	case int64:
		me.Value = float64(v)
	case uint:
		me.Value = float64(v)
	case uint8:
		me.Value = float64(v)
	case uint16:
		me.Value = float64(v)
	case uint32:
		me.Value = float64(v)
	case uint64:
		me.Value = float64(v)
	case string:
		var err error
		me.Value, err = strconv.ParseFloat(v, 64)
		if err != nil {
			qlog.Error("Failed to parse string to float: %s, error: %v", v, err)
			return
		}
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
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

func (me *ValueFloat) AsString() string {
	return fmt.Sprintf("%f", me.Value)
}
