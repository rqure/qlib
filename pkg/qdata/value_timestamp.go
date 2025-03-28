package qdata

import (
	"time"

	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ValueTimestamp struct {
	Value time.Time
}

func NewTimestamp(v ...time.Time) *Value {
	me := &ValueTimestamp{
		Value: time.Time{},
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: new(ValueType).As(VTTimestamp),
		ValueConstructor:  me,
		AnyPbConverter:    me,
		StringConverter:   me,
		RawProvider:       me,
		RawReceiver:       me,
		TimestampProvider: me,
		TimestampReceiver: me,
	}
}

func (me *ValueTimestamp) GetTimestamp() time.Time {
	return me.Value
}

func (me *ValueTimestamp) SetTimestamp(value time.Time) {
	me.Value = value
}

func (me *ValueTimestamp) GetRaw() interface{} {
	return me.Value
}

func (me *ValueTimestamp) SetRaw(value interface{}) {
	if v, ok := value.(time.Time); ok {
		me.Value = v
	}
}

func (me *ValueTimestamp) Clone() *Value {
	return NewTimestamp(me.Value)
}

func (me *ValueTimestamp) AsAnyPb() *anypb.Any {
	ts := timestamppb.New(me.Value)
	a, err := anypb.New(&qprotobufs.Timestamp{
		Raw: ts,
	})

	if err != nil {
		return nil
	}

	return a
}

func (me *ValueTimestamp) AsString() string {
	return me.Value.Format(time.RFC3339)
}
