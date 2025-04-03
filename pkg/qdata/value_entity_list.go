package qdata

import (
	"strings"

	"github.com/rqure/qlib/pkg/qlog"
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
		StringConverter:    me,
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
	switch v := value.(type) {
	case []EntityId:
		me.Value = v
	case []string:
		me.Value = CastStringSliceToEntityIdSlice(v)
	case string:
		me.Value = CastStringSliceToEntityIdSlice(strings.Split(v, ","))
	case []interface{}:
		me.Value = make([]EntityId, len(v))
		for i, item := range v {
			switch item := item.(type) {
			case string:
				me.Value[i] = EntityId(item)
			case EntityId:
				me.Value[i] = item
			default:
				qlog.Error("Invalid type for SetRaw subitem: %T", item)
			}
		}
	default:
		qlog.Error("Invalid type for SetRaw: %T", v)
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

func (me *ValueEntityList) AsString() string {
	return strings.Join(CastEntityIdSliceToStringSlice(me.Value), ",")
}
