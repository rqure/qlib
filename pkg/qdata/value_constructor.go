package qdata

import (
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

func NewValue(vType ValueType, args ...interface{}) *Value {
	var value *Value

	switch vType {
	case Int:
		value = NewInt()
	case Float:
		value = NewFloat()
	case String:
		value = NewString()
	case EntityReference:
		value = NewEntityReference()
	case Timestamp:
		value = NewTimestamp()
	case Bool:
		value = NewBool()
	case BinaryFile:
		value = NewBinaryFile()
	case Choice:
		value = NewChoice()
	case EntityList:
		value = NewEntityList()
	}

	if value != nil && len(args) > 0 {
		value.SetRaw(args[0])
	}

	return value
}

func NewValueFromAnyPb(a *anypb.Any) *Value {
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
