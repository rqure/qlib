package request

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Wrapper struct {
	impl *protobufs.DatabaseRequest
}

func New() data.Request {
	return &Wrapper{
		impl: &protobufs.DatabaseRequest{},
	}
}

func FromPb(impl *protobufs.DatabaseRequest) data.Request {
	return &Wrapper{
		impl: impl,
	}
}

func FromField(f data.Field) data.Request {
	return New().SetEntityId(f.GetEntityId()).SetFieldName(f.GetFieldName()).SetValue(f.GetValue()).SetSuccessful(false)
}

func (r *Wrapper) GetEntityId() string {
	return r.impl.Id
}

func (r *Wrapper) GetFieldName() string {
	return r.impl.Field
}

func (r *Wrapper) GetWriteTime() time.Time {
	if r.impl.WriteTime == nil {
		return time.Time{}
	}

	return r.impl.WriteTime.Raw.AsTime()
}

func (r *Wrapper) GetWriter() string {
	if r.impl.WriterId == nil {
		return ""
	}

	return r.impl.WriterId.Raw
}

func (r *Wrapper) IsSuccessful() bool {
	return r.impl.Success
}

func (r *Wrapper) GetValue() data.Value {
	return field.FromAnyPb(r.impl.Value)
}

func (r *Wrapper) SetEntityId(id string) data.Request {
	r.impl.Id = id
	return r
}

func (r *Wrapper) SetFieldName(name string) data.Request {
	r.impl.Field = name
	return r
}

func (r *Wrapper) SetWriteTime(t time.Time) data.Request {
	r.impl.WriteTime = &protobufs.Timestamp{
		Raw: timestamppb.New(t),
	}
	return r
}

func (r *Wrapper) SetWriter(id string) data.Request {
	r.impl.WriterId = &protobufs.String{
		Raw: id,
	}

	return r
}

func (r *Wrapper) SetSuccessful(success bool) data.Request {
	r.impl.Success = success
	return r
}

func (r *Wrapper) SetValue(v data.Value) data.Request {
	r.impl.Value = field.ToAnyPb(v)
	return r
}
