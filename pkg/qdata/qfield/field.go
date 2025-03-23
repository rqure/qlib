package qfield

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Field struct {
	impl *qprotobufs.DatabaseField
}

func ToFieldPb(f qdata.Field) *qprotobufs.DatabaseField {
	if f == nil {
		return nil
	}

	switch c := f.(type) {
	case *Field:
		return c.impl
	default:
		return nil
	}
}

func FromFieldPb(impl *qprotobufs.DatabaseField) qdata.Field {
	return &Field{
		impl: impl,
	}
}

func FromRequest(r qdata.Request) qdata.Field {
	writeTime := time.Unix(0, 0)
	writer := ""

	if r.GetWriteTime() != nil {
		writeTime = *r.GetWriteTime()
	}

	if r.GetWriter() != nil {
		writer = *r.GetWriter()
	}

	return &Field{
		impl: &qprotobufs.DatabaseField{
			Id:        r.GetEntityId(),
			Name:      r.GetFieldName(),
			Value:     ToAnyPb(r.GetValue()),
			WriteTime: timestamppb.New(writeTime),
			WriterId:  writer,
		},
	}
}

func (f *Field) GetValue() qdata.ValueTypeProvider {
	return FromAnyPb(&f.impl.Value)
}

func (f *Field) GetWriteTime() time.Time {
	if f.impl == nil {
		qlog.Error("Impl not defined")
		return time.Unix(0, 0)
	}

	if f.impl.WriteTime == nil {
		qlog.Error("Writetime not defined")
		return time.Unix(0, 0)
	}

	return f.impl.WriteTime.AsTime()
}

func (f *Field) GetWriter() string {
	if f.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return f.impl.WriterId
}

func (f *Field) GetEntityId() string {
	if f.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return f.impl.Id
}

func (f *Field) GetFieldName() string {
	if f.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return f.impl.Name
}
