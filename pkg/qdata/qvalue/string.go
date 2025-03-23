package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type String struct {
	Value string
}

func NewString(v ...string) qdata.ModifiableValue {
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
		RawValueProvider:      me,
		ModifiableStringValue: me,
	}
}

func (me *String) GetString() string {
	return me.Value
}

func (me *String) SetString(value string) qdata.ModifiableStringValue {
	me.Value = value
	return me
}

func (me *String) Raw() interface{} {
	return me.Value
}
