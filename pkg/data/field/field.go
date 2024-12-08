package field

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Field struct {
	impl *protobufs.DatabaseField
}

func ToFieldPb(f data.Field) *protobufs.DatabaseField {
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

func FromFieldPb(impl *protobufs.DatabaseField) data.Field {
	return &Field{
		impl: impl,
	}
}

func FromRequest(r data.Request) data.Field {
	writeTime := time.Unix(0, 0)
	writer := ""

	if r.GetWriteTime() != nil {
		writeTime = *r.GetWriteTime()
	}

	if r.GetWriter() != nil {
		writer = *r.GetWriter()
	}

	return &Field{
		impl: &protobufs.DatabaseField{
			Id:        r.GetEntityId(),
			Name:      r.GetFieldName(),
			Value:     ToAnyPb(r.GetValue()),
			WriteTime: timestamppb.New(writeTime),
			WriterId:  writer,
		},
	}
}

func (f *Field) GetValue() data.Value {
	return FromAnyPb(f.impl.Value)
}

func (f *Field) GetWriteTime() time.Time {
	return f.impl.WriteTime.AsTime()
}

func (f *Field) GetWriter() string {
	return f.impl.WriterId
}

func (f *Field) GetEntityId() string {
	return f.impl.Id
}

func (f *Field) GetFieldName() string {
	return f.impl.Name
}
