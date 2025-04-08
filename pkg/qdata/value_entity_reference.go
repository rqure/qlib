package qdata

import (
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueEntityReference struct {
	Value EntityId
}

func NewEntityReference(v ...EntityId) *Value {
	me := &ValueEntityReference{
		Value: "",
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider:       new(ValueType).As(VTEntityReference),
		ValueConstructor:        me,
		AnyPbConverter:          me,
		StringConverter:         me,
		RawProvider:             me,
		RawReceiver:             me,
		EntityReferenceProvider: me,
		EntityReferenceReceiver: me,
	}
}

func (me *ValueEntityReference) GetEntityReference() EntityId {
	return me.Value
}

func (me *ValueEntityReference) SetEntityReference(value EntityId) {
	me.Value = value
}

func (me *ValueEntityReference) GetRaw() any {
	return me.Value
}

func (me *ValueEntityReference) SetRaw(value any) {
	switch v := value.(type) {
	case EntityId:
		me.Value = v
	case *EntityId:
		if v != nil {
			me.Value = *v
		}
	case string:
		me.Value = EntityId(v)
	case *string:
		if v != nil {
			me.Value = EntityId(*v)
		}
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
	}
}

func (me *ValueEntityReference) Clone() *Value {
	return NewEntityReference(me.Value)
}

func (me *ValueEntityReference) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.EntityReference{
		Raw: me.Value.AsString(),
	})

	if err != nil {
		return nil
	}

	return a
}

func (me *ValueEntityReference) AsString() string {
	return me.Value.AsString()
}
