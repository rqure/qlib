package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueEntityReference struct {
	Value string
}

func NewEntityReference(v ...string) *Value {
	me := &ValueEntityReference{
		Value: "",
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider:       new(ValueType).As(EntityReference),
		ValueConstructor:        me,
		AnyPbConverter:          me,
		RawProvider:             me,
		RawReceiver:             me,
		EntityReferenceProvider: me,
		EntityReferenceReceiver: me,
	}
}

func (me *ValueEntityReference) GetEntityReference() string {
	return me.Value
}

func (me *ValueEntityReference) SetEntityReference(value string) {
	me.Value = value
}

func (me *ValueEntityReference) GetRaw() interface{} {
	return me.Value
}

func (me *ValueEntityReference) SetRaw(value interface{}) {
	if v, ok := value.(string); ok {
		me.Value = v
	}
}

func (me *ValueEntityReference) Clone() *Value {
	return NewEntityReference(me.Value)
}

func (me *ValueEntityReference) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.EntityReference{
		Raw: me.Value,
	})

	if err != nil {
		return nil
	}

	return a
}
