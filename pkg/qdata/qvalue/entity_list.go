package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type EntityList struct {
	Value []string
}

func NewEntityList(v ...[]string) qdata.ModifiableValue {
	me := &EntityList{
		Value: []string{}, // Empty list as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.EntityListType,
		},
		RawValueProvider:          me,
		ModifiableEntityListValue: me,
	}
}

func (me *EntityList) GetEntityList() []string {
	return append([]string(nil), me.Value...)
}

func (me *EntityList) SetEntityList(value []string) qdata.ModifiableEntityListValue {
	me.Value = value
	return me
}

func (me *EntityList) Raw() interface{} {
	return me.Value
}
