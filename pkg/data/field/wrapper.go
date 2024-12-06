package field

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type Wrapper struct {
	impl *protobufs.DatabaseField
}

func FromPb(impl *protobufs.DatabaseField) data.Field {
	return &Wrapper{
		impl: impl,
	}
}

func (f *Wrapper) GetValue(m proto.Message) proto.Message {
	if err := f.impl.Value.UnmarshalTo(m); err != nil {
		return m
	}

	return m
}

func (f *Wrapper) GetInt() int64 {
	return f.GetValue(new(protobufs.Int)).(*protobufs.Int).GetRaw()
}

func (f *Wrapper) GetFloat() float64 {
	return f.GetValue(new(protobufs.Float)).(*protobufs.Float).GetRaw()
}

func (f *Wrapper) GetString() string {
	return f.GetValue(new(protobufs.String)).(*protobufs.String).GetRaw()
}

func (f *Wrapper) GetBool() bool {
	return f.GetValue(new(protobufs.Bool)).(*protobufs.Bool).GetRaw()
}

func (f *Wrapper) GetBinaryFile() string {
	return f.GetValue(new(protobufs.BinaryFile)).(*protobufs.BinaryFile).GetRaw()
}

func (f *Wrapper) GetEntityReference() string {
	return f.GetValue(new(protobufs.EntityReference)).(*protobufs.EntityReference).GetRaw()
}

func (f *Wrapper) GetTimestamp() time.Time {
	return f.GetValue(new(protobufs.Timestamp)).(*protobufs.Timestamp).GetRaw().AsTime()
}

func (f *Wrapper) GetTransformation() string {
	return f.GetValue(new(protobufs.Transformation)).(*protobufs.Transformation).GetRaw()
}

func (f *Wrapper) GetWriteTime() time.Time {
	return f.impl.WriteTime.AsTime()
}

func (f *Wrapper) GetWriter() string {
	return f.impl.WriterId
}

func (f *Wrapper) GetEntityId() string {
	return f.impl.Id
}

func (f *Wrapper) GetEntityName() string {
	return f.impl.Name
}
