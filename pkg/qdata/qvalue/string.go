package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type String struct {
	Value string
}

func NewString(v ...string) qdata.Value {
	me := &String{
		Value: "",
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.StringType,
		},
		RawProvider:    me,
		RawReceiver:    me,
		StringProvider: me,
		StringReceiver: me,
	}
}

func (me *String) GetString() string {
	return me.Value
}

func (me *String) SetString(value string) {
	me.Value = value
}

func (me *String) GetRaw() interface{} {
	return me.Value
}

func (me *String) SetRaw(value interface{}) {
	if v, ok := value.(string); ok {
		me.Value = v
	}
}
