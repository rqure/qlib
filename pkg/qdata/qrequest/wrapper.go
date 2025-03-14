package qrequest

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Wrapper struct {
	impl *qprotobufs.DatabaseRequest
}

func New() qdata.Request {
	return &Wrapper{
		impl: &qprotobufs.DatabaseRequest{},
	}
}

func FromPb(impl *qprotobufs.DatabaseRequest) qdata.Request {
	return &Wrapper{
		impl: impl,
	}
}

func FromField(f qdata.Field) qdata.Request {
	return New().SetEntityId(f.GetEntityId()).SetFieldName(f.GetFieldName()).SetValue(f.GetValue())
}

func ToPb(r qdata.Request) *qprotobufs.DatabaseRequest {
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

func (r *Wrapper) GetValue() qdata.Value {
	return qfield.FromAnyPb(&r.impl.Value)
}

func (r *Wrapper) SetEntityId(id string) qdata.Request {
	r.impl.Id = id
	return r
}

func (r *Wrapper) SetFieldName(name string) qdata.Request {
	r.impl.Field = name
	return r
}

func (r *Wrapper) SetWriteTime(t *time.Time) qdata.Request {
	if t == nil {
		r.impl.WriteTime = nil
		return r
	}

	r.impl.WriteTime = &qprotobufs.Timestamp{
		Raw: timestamppb.New(*t),
	}
	return r
}

func (r *Wrapper) SetWriter(id *string) qdata.Request {
	if id == nil {
		r.impl.WriterId = nil
		return r
	}

	r.impl.WriterId = &qprotobufs.String{
		Raw: *id,
	}

	return r
}

func (r *Wrapper) SetSuccessful(success bool) qdata.Request {
	r.impl.Success = success
	return r
}

func (r *Wrapper) SetValue(v qdata.Value) qdata.Request {
	r.impl.Value = qfield.ToAnyPb(v)
	return r
}

func (r *Wrapper) GetWriteOpt() qdata.WriteOpt {
	switch r.impl.WriteOpt {
	case qprotobufs.DatabaseRequest_WRITE_CHANGES:
		return qdata.WriteChanges
	default:
		return qdata.WriteNormal
	}
}

func (r *Wrapper) SetWriteOpt(opt qdata.WriteOpt) qdata.Request {
	switch opt {
	case qdata.WriteChanges:
		r.impl.WriteOpt = qprotobufs.DatabaseRequest_WRITE_CHANGES
	default:
		r.impl.WriteOpt = qprotobufs.DatabaseRequest_WRITE_NORMAL
	}
	return r
}

func (r *Wrapper) Clone() qdata.Request {
	var writeTime *qprotobufs.Timestamp
	if r.impl.WriteTime != nil {
		writeTime = &qprotobufs.Timestamp{
			Raw: r.impl.WriteTime.Raw,
		}
	}

	var writerId *qprotobufs.String
	if r.impl.WriterId != nil {
		writerId = &qprotobufs.String{
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
		impl: &qprotobufs.DatabaseRequest{
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
