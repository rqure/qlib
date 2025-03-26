package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueEntityList struct {
	Value []EntityId
}

func NewEntityList(v ...[]EntityId) *Value {
	me := &ValueEntityList{
		Value: []EntityId{}, // Empty list as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider:  new(ValueType).As(VTEntityList),
		ValueConstructor:   me,
		AnyPbConverter:     me,
		RawProvider:        me,
		RawReceiver:        me,
		EntityListProvider: me,
		EntityListReceiver: me,
	}
}

func (me *ValueEntityList) GetEntityList() []EntityId {
	return append([]EntityId{}, me.Value...)
}

func (me *ValueEntityList) SetEntityList(value []EntityId) {
	me.Value = value
}

func (me *ValueEntityList) GetRaw() interface{} {
	return me.Value
}

func (me *ValueEntityList) SetRaw(value interface{}) {
	if v, ok := value.([]EntityId); ok {
		me.Value = v
	}
}

func (me *ValueEntityList) Clone() *Value {
	return NewEntityList(me.Value)
}

func (me *ValueEntityList) AsAnyPb() *anypb.Any {
	list := make([]string, 0, len(me.Value))

	for _, v := range me.Value {
		list = append(list, v.AsString())
	}

	a, err := anypb.New(&qprotobufs.EntityList{
		Raw: list,
	})

	if err != nil {
		return nil
	}

	return a
}
