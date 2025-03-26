package qdata

import (
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
	GetEntityReference() string
}

type TimestampProvider interface {
	GetTimestamp() time.Time
}

type ChoiceProvider interface {
	GetChoice() int
}

type EntityListProvider interface {
	GetEntityList() []string
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
	SetEntityReference(value string)
}

type TimestampReceiver interface {
	SetTimestamp(value time.Time)
}

type ChoiceReceiver interface {
	SetChoice(value int)
}

type EntityListReceiver interface {
	SetEntityList(value []string)
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

func (me *Value) Update(o *Value) {
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
	me.Update(NewInt(v))
	return me
}

func (me *Value) FromFloat(v float64) *Value {
	me.Update(NewFloat(v))
	return me
}

func (me *Value) FromString(v string) *Value {
	me.Update(NewString(v))
	return me
}

func (me *Value) FromBool(v bool) *Value {
	me.Update(NewBool(v))
	return me
}

func (me *Value) FromBinaryFile(v string) *Value {
	me.Update(NewBinaryFile(v))
	return me
}

func (me *Value) FromEntityReference(v string) *Value {
	me.Update(NewEntityReference(v))
	return me
}

func (me *Value) FromTimestamp(v time.Time) *Value {
	me.Update(NewTimestamp(v))
	return me
}

func (me *Value) FromChoice(v int) *Value {
	me.Update(NewChoice(v))
	return me
}

func (me *Value) FromEntityList(v []string) *Value {
	me.Update(NewEntityList(v))
	return me
}

func (me *Value) FromAnyPb(a *anypb.Any) *Value {
	if a.MessageIs(&qprotobufs.Int{}) {
		m := new(qprotobufs.Int)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewInt(int(m.Raw))
	}

	if a.MessageIs(&qprotobufs.Float{}) {
		m := new(qprotobufs.Float)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewFloat(m.Raw)
	}

	if a.MessageIs(&qprotobufs.String{}) {
		m := new(qprotobufs.String)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewString(m.Raw)
	}

	if a.MessageIs(&qprotobufs.EntityReference{}) {
		m := new(qprotobufs.EntityReference)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewEntityReference(m.Raw)
	}

	if a.MessageIs(&qprotobufs.Timestamp{}) {
		m := new(qprotobufs.Timestamp)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewTimestamp(m.Raw.AsTime())
	}

	if a.MessageIs(&qprotobufs.Bool{}) {
		m := new(qprotobufs.Bool)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewBool(m.Raw)
	}

	if a.MessageIs(&qprotobufs.BinaryFile{}) {
		m := new(qprotobufs.BinaryFile)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewBinaryFile(m.Raw)
	}

	if a.MessageIs(&qprotobufs.Choice{}) {
		m := new(qprotobufs.Choice)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewChoice(int(m.Raw))
	}

	if a.MessageIs(&qprotobufs.EntityList{}) {
		m := new(qprotobufs.EntityList)
		if err := a.UnmarshalTo(m); err != nil {
			return nil
		}
		return NewEntityList(m.Raw)
	}

	return nil
}
