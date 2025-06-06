package qdata

import (
	"fmt"
	"strconv"

	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueChoice struct {
	Value int
}

func NewChoice(v ...int) *Value {
	me := &ValueChoice{
		Value: 0, // Default choice index
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: new(ValueType).As(VTChoice),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		StringConverter:   me,
		RawProvider:       me,
		RawReceiver:       me,
		ChoiceProvider:    me,
		ChoiceReceiver:    me,
	}
}

func (me *ValueChoice) GetChoice() int {
	return me.Value
}

func (me *ValueChoice) SetChoice(value int) {
	me.Value = value
}

func (me *ValueChoice) GetRaw() any {
	return me.Value
}

func (me *ValueChoice) SetRaw(value any) {
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
			qlog.Error("Invalid string for SetRaw: %s, error: %v\n", v, err)
			return
		}
	default:
		qlog.Error("Invalid type for SetRaw: %T\n", v)
	}
}

func (me *ValueChoice) Clone() *Value {
	return NewChoice(me.Value)
}

func (me *ValueChoice) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.Choice{
		Raw: int64(me.Value),
	})

	if err != nil {
		return nil
	}

	return a
}

func (me *ValueChoice) AsString() string {
	return fmt.Sprintf("%d", me.Value)
}
