package qvalue

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
)

type Timestamp struct {
	Value time.Time
}

func NewTimestamp(v ...time.Time) qdata.Value {
	me := &Timestamp{
		Value: time.Time{},
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.TimestampType,
		},
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
