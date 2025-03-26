package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type ValueEntityList struct {
	Value []string
}

func NewEntityList(v ...[]string) *Value {
	me := &ValueEntityList{
		Value: []string{}, // Empty list as default
	}

	if len(v) > 0 {
		me.Value = v[0]
	}

	return &Value{
		ValueTypeProvider:  new(ValueType).As(EntityList),
		ValueConstructor:   me,
		AnyPbConverter:     me,
		RawProvider:        me,
		RawReceiver:        me,
		EntityListProvider: me,
		EntityListReceiver: me,
	}
}

func (me *ValueEntityList) GetEntityList() []string {
	return append([]string(nil), me.Value...)
}

func (me *ValueEntityList) SetEntityList(value []string) {
	me.Value = value
}

func (me *ValueEntityList) GetRaw() interface{} {
	return me.Value
}

func (me *ValueEntityList) SetRaw(value interface{}) {
	if v, ok := value.([]string); ok {
		me.Value = v
	}
}

func (me *ValueEntityList) Clone() *Value {
	return NewEntityList(me.Value)
}

func (me *ValueEntityList) AsAnyPb() *anypb.Any {
	a, err := anypb.New(&qprotobufs.EntityList{
		Raw: me.GetEntityList(),
	})

	if err != nil {
		return nil
	}

	return a
}
