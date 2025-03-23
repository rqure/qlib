package qvalue

import "github.com/rqure/qlib/pkg/qdata"

type EntityReference struct {
	Value string
}

func NewEntityReference(v ...string) qdata.ModifiableValue {
	me := &EntityReference{
		Value: "", // Empty reference as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider: &ValueTypeProvider{
			ValueType: qdata.EntityReferenceType,
		},
		RawValueProvider:               me,
		ModifiableEntityReferenceValue: me,
	}
}

func (me *EntityReference) GetEntityReference() string {
	return me.Value
}

func (me *EntityReference) SetEntityReference(value string) qdata.ModifiableEntityReferenceValue {
	me.Value = value
	return me
}

func (me *EntityReference) Raw() interface{} {
	return me.Value
}
