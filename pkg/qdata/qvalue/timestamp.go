package qvalue

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Timestamp struct {
	Value time.Time
}

func NewTimestamp(v ...time.Time) *qdata.Value {
	me := &Timestamp{
		Value: time.Time{},
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.Timestamp,
		},
		ValueConstructor:  me,
		AnyPbConverter:    me,
		RawProvider:       me,
		RawReceiver:       me,
		TimestampProvider: me,
		TimestampReceiver: me,
	}
}

func (me *Timestamp) GetTimestamp() time.Time {
	return me.Value
}

func (me *Timestamp) SetTimestamp(value time.Time) {
	me.Value = value
}

func (me *Timestamp) GetRaw() interface{} {
	return me.Value
}

func (me *Timestamp) SetRaw(value interface{}) {
	if v, ok := value.(time.Time); ok {
		me.Value = v
	}
}

func (me *Timestamp) Clone() *qdata.Value {
	return NewTimestamp(me.Value)
}

func (me *Timestamp) AsAnyPb() *anypb.Any {
	ts := timestamppb.New(me.Value)
	a, err := anypb.New(&qprotobufs.Timestamp{
		Raw: ts,
	})

	if err != nil {
		return nil
	}

	return a
}
