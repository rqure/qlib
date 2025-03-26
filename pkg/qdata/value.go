package qdata

import (
	"slices"
	"time"

	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

type RawProvider interface {
	GetRaw() interface{}
}

type RawReceiver interface {
	SetRaw(value interface{})
}

type IntProvider interface {
	GetInt() int
}

type FloatProvider interface {
	GetFloat() float64
}

type StringProvider interface {
	GetString() string
}

type BoolProvider interface {
	GetBool() bool
}

type BinaryFileProvider interface {
	GetBinaryFile() string
}

type EntityReferenceProvider interface {
	GetEntityReference() EntityId
}

type TimestampProvider interface {
	GetTimestamp() time.Time
}

type ChoiceProvider interface {
	GetChoice() int
}

type EntityListProvider interface {
	GetEntityList() []EntityId
}

type IntReceiver interface {
	SetInt(value int)
}

type FloatReceiver interface {
	SetFloat(value float64)
}

type StringReceiver interface {
	SetString(value string)
}

type BoolReceiver interface {
	SetBool(value bool)
}

type BinaryFileReceiver interface {
	SetBinaryFile(value string)
}

type EntityReferenceReceiver interface {
	SetEntityReference(value EntityId)
}

type TimestampReceiver interface {
	SetTimestamp(value time.Time)
}

type ChoiceReceiver interface {
	SetChoice(value int)
}

type EntityListReceiver interface {
	SetEntityList(value []EntityId)
}

type ValueConstructor interface {
	Clone() *Value
}

type AnyPbConverter interface {
	AsAnyPb() *anypb.Any
}

type Value struct {
	ValueConstructor
	AnyPbConverter
	ValueTypeProvider

	RawProvider
	IntProvider
	FloatProvider
	StringProvider
	BoolProvider
	BinaryFileProvider
	EntityReferenceProvider
	TimestampProvider
	ChoiceProvider
	EntityListProvider

	RawReceiver
	IntReceiver
	FloatReceiver
	StringReceiver
	BoolReceiver
	BinaryFileReceiver
	EntityReferenceReceiver
	TimestampReceiver
	ChoiceReceiver
	EntityListReceiver
}

func (me *Value) FromValue(o *Value) {
	me.ValueConstructor = o.ValueConstructor
	me.AnyPbConverter = o.AnyPbConverter
	me.ValueTypeProvider = o.ValueTypeProvider
	me.RawProvider = o.RawProvider
	me.IntProvider = o.IntProvider
	me.FloatProvider = o.FloatProvider
	me.StringProvider = o.StringProvider
	me.BoolProvider = o.BoolProvider
	me.BinaryFileProvider = o.BinaryFileProvider
	me.EntityReferenceProvider = o.EntityReferenceProvider
	me.TimestampProvider = o.TimestampProvider
	me.ChoiceProvider = o.ChoiceProvider
	me.EntityListProvider = o.EntityListProvider
	me.RawReceiver = o.RawReceiver
	me.IntReceiver = o.IntReceiver
	me.FloatReceiver = o.FloatReceiver
	me.StringReceiver = o.StringReceiver
	me.BoolReceiver = o.BoolReceiver
	me.BinaryFileReceiver = o.BinaryFileReceiver
	me.EntityReferenceReceiver = o.EntityReferenceReceiver
	me.TimestampReceiver = o.TimestampReceiver
	me.ChoiceReceiver = o.ChoiceReceiver
	me.EntityListReceiver = o.EntityListReceiver
}

func (me *Value) FromInt(v int) *Value {
	me.FromValue(NewInt(v))
	return me
}

func (me *Value) FromFloat(v float64) *Value {
	me.FromValue(NewFloat(v))
	return me
}

func (me *Value) FromString(v string) *Value {
	me.FromValue(NewString(v))
	return me
}

func (me *Value) FromBool(v bool) *Value {
	me.FromValue(NewBool(v))
	return me
}

func (me *Value) FromBinaryFile(v string) *Value {
	me.FromValue(NewBinaryFile(v))
	return me
}

func (me *Value) FromEntityReference(v EntityId) *Value {
	me.FromValue(NewEntityReference(v))
	return me
}

func (me *Value) FromTimestamp(v time.Time) *Value {
	me.FromValue(NewTimestamp(v))
	return me
}

func (me *Value) FromChoice(v int) *Value {
	me.FromValue(NewChoice(v))
	return me
}

func (me *Value) FromEntityList(v []EntityId) *Value {
	me.FromValue(NewEntityList(v))
	return me
}

func (me *Value) FromAnyPb(a *anypb.Any) *Value {
	if a.MessageIs(&qprotobufs.Int{}) {
		m := new(qprotobufs.Int)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromInt(int(m.Raw))
		}
	}

	if a.MessageIs(&qprotobufs.Float{}) {
		m := new(qprotobufs.Float)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromFloat(m.Raw)
		}
	}

	if a.MessageIs(&qprotobufs.String{}) {
		m := new(qprotobufs.String)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromString(m.Raw)
		}
	}

	if a.MessageIs(&qprotobufs.EntityReference{}) {
		m := new(qprotobufs.EntityReference)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromEntityReference(EntityId(m.Raw))
		}
	}

	if a.MessageIs(&qprotobufs.Timestamp{}) {
		m := new(qprotobufs.Timestamp)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromTimestamp(m.Raw.AsTime())
		}
	}

	if a.MessageIs(&qprotobufs.Bool{}) {
		m := new(qprotobufs.Bool)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromBool(m.Raw)
		}
	}

	if a.MessageIs(&qprotobufs.BinaryFile{}) {
		m := new(qprotobufs.BinaryFile)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromBinaryFile(m.Raw)
		}
	}

	if a.MessageIs(&qprotobufs.Choice{}) {
		m := new(qprotobufs.Choice)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromChoice(int(m.Raw))
		}
	}

	if a.MessageIs(&qprotobufs.EntityList{}) {
		m := new(qprotobufs.EntityList)
		if err := a.UnmarshalTo(m); err == nil {
			me.FromEntityList(CastStringSliceToEntityIdSlice(m.Raw))
		}
	}

	return me
}

func (me *Value) Init() *Value {
	me.ValueTypeProvider = new(ValueType)

	return me
}

func (me *Value) Equals(o *Value) bool {
	if me.ValueTypeProvider != o.ValueTypeProvider {
		return false
	}

	if me.ValueTypeProvider == nil {
		return false
	}

	if me.Type() != o.Type() {
		return false
	}

	switch me.Type() {
	case VTInt:
		return me.GetInt() == o.GetInt()
	case VTFloat:
		return me.GetFloat() == o.GetFloat()
	case VTString:
		return me.GetString() == o.GetString()
	case VTBool:
		return me.GetBool() == o.GetBool()
	case VTBinaryFile:
		return me.GetBinaryFile() == o.GetBinaryFile()
	case VTEntityReference:
		return me.GetEntityReference() == o.GetEntityReference()
	case VTTimestamp:
		return me.GetTimestamp().Equal(o.GetTimestamp())
	case VTChoice:
		return me.GetChoice() == o.GetChoice()
	case VTEntityList:
		return slices.Equal(me.GetEntityList(), o.GetEntityList())
	}

	return false
}
