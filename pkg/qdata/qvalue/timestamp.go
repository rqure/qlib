package qvalue

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
)

type Timestamp struct {
	Value time.Time
}

func NewTimestamp(v ...time.Time) qdata.ModifiableValue {
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
		RawValueProvider:         me,
		ModifiableTimestampValue: me,
	}
}

func (me *Timestamp) GetTimestamp() time.Time {
	return me.Value
}

func (me *Timestamp) SetTimestamp(value time.Time) qdata.ModifiableTimestampValue {
	me.Value = value
	return me
}

func (me *Timestamp) Raw() interface{} {
	return me.Value
}
