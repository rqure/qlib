package qvalue

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
)

func New(vType qdata.ValueType) *qdata.Value {
	switch vType {
	case qdata.IntType:
		return NewInt()
	case qdata.FloatType:
		return NewFloat()
	case qdata.StringType:
		return NewString()
	case qdata.EntityReferenceType:
		return NewEntityReference()
	case qdata.TimestampType:
		return NewTimestamp()
	case qdata.BoolType:
		return NewBool()
	case qdata.BinaryFileType:
		return NewBinaryFile()
	case qdata.ChoiceType:
		return NewChoice()
	case qdata.EntityListType:
		return NewEntityList()
	}

	return nil
}

func FromAnyPb(a *anypb.Any) *qdata.Value {
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
