package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type EntityReference struct {
	Value string
}

func NewEntityReference(v ...string) *qdata.Value {
	me := &EntityReference{
		Value: "",
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &qdata.Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.EntityReferenceType,
		},
		ValueConstructor:        me,
		RawProvider:             me,
		RawReceiver:             me,
		EntityReferenceProvider: me,
		EntityReferenceReceiver: me,
	}
}

func (me *EntityReference) GetEntityReference() string {
	return me.Value
}

func (me *EntityReference) SetEntityReference(value string) {
	me.Value = value
}

func (me *EntityReference) GetRaw() interface{} {
	return me.Value
}

func (me *EntityReference) SetRaw(value interface{}) {
	if v, ok := value.(string); ok {
		me.Value = v
	}
}

func (me *EntityReference) Clone() *qdata.Value {
	return NewEntityReference(me.Value)
}
