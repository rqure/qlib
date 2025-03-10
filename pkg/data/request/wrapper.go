package request

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/types/known/anypb"
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
	return New().SetEntityId(f.GetEntityId()).SetFieldName(f.GetFieldName()).SetValue(f.GetValue())
}

func ToPb(r data.Request) *protobufs.DatabaseRequest {
	return r.(*Wrapper).impl
}

func (r *Wrapper) GetEntityId() string {
	return r.impl.Id
}

func (r *Wrapper) GetFieldName() string {
	return r.impl.Field
}

func (r *Wrapper) GetWriteTime() *time.Time {
	if r.impl.WriteTime == nil {
		return nil
	}

	if r.impl.WriteTime.Raw == nil {
		return nil
	}

	t := r.impl.WriteTime.Raw.AsTime()
	return &t
}

func (r *Wrapper) GetWriter() *string {
	if r.impl.WriterId == nil {
		return nil
	}

	s := r.impl.WriterId.Raw
	return &s
}

func (r *Wrapper) IsSuccessful() bool {
	return r.impl.Success
}

func (r *Wrapper) GetValue() data.Value {
	return field.FromAnyPb(&r.impl.Value)
}

func (r *Wrapper) SetEntityId(id string) data.Request {
	r.impl.Id = id
	return r
}

func (r *Wrapper) SetFieldName(name string) data.Request {
	r.impl.Field = name
	return r
}

func (r *Wrapper) SetWriteTime(t *time.Time) data.Request {
	if t == nil {
		r.impl.WriteTime = nil
		return r
	}

	r.impl.WriteTime = &protobufs.Timestamp{
		Raw: timestamppb.New(*t),
	}
	return r
}

func (r *Wrapper) SetWriter(id *string) data.Request {
	if id == nil {
		r.impl.WriterId = nil
		return r
	}

	r.impl.WriterId = &protobufs.String{
		Raw: *id,
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

func (r *Wrapper) GetWriteOpt() data.WriteOpt {
	switch r.impl.WriteOpt {
	case protobufs.DatabaseRequest_WRITE_CHANGES:
		return data.WriteChanges
	default:
		return data.WriteNormal
	}
}

func (r *Wrapper) SetWriteOpt(opt data.WriteOpt) data.Request {
	switch opt {
	case data.WriteChanges:
		r.impl.WriteOpt = protobufs.DatabaseRequest_WRITE_CHANGES
	default:
		r.impl.WriteOpt = protobufs.DatabaseRequest_WRITE_NORMAL
	}
	return r
}

func (r *Wrapper) Clone() data.Request {
	var writeTime *protobufs.Timestamp
	if r.impl.WriteTime != nil {
		writeTime = &protobufs.Timestamp{
			Raw: r.impl.WriteTime.Raw,
		}
	}

	var writerId *protobufs.String
	if r.impl.WriterId != nil {
		writerId = &protobufs.String{
			Raw: r.impl.WriterId.Raw,
		}
	}

	var value *anypb.Any
	if r.impl.Value != nil {
		value = &anypb.Any{
			TypeUrl: r.impl.Value.TypeUrl,
			Value:   []byte{},
		}

		copy(value.Value, r.impl.Value.Value)
	}

	return &Wrapper{
		impl: &protobufs.DatabaseRequest{
			Id:        r.impl.Id,
			Field:     r.impl.Field,
			WriteTime: writeTime,
			WriterId:  writerId,
			Value:     value,
			Success:   r.impl.Success,
			WriteOpt:  r.impl.WriteOpt,
		},
	}
}
