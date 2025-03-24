package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type EntityList struct {
	Value []string
}

func NewEntityList(v ...[]string) *qdata.Value {
	me := &EntityList{
		Value: []string{}, // Empty list as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.EntityListType,
		},
		ValueConstructor:   me,
		RawProvider:        me,
		RawReceiver:        me,
		EntityListProvider: me,
		EntityListReceiver: me,
	}
}

func (me *EntityList) GetEntityList() []string {
	return append([]string(nil), me.Value...)
}

func (me *EntityList) SetEntityList(value []string) {
	me.Value = value
}

func (me *EntityList) GetRaw() interface{} {
	return me.Value
}

func (me *EntityList) SetRaw(value interface{}) {
	if v, ok := value.([]string); ok {
		me.Value = v
	}
}

func (me *EntityList) Clone() *qdata.Value {
	return NewEntityList(me.Value)
}
