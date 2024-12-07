package request

import (
	"time"

	"github.com/rqure/qlib/pkg/protobufs"
)

type Wrapper struct {
	impl *protobufs.DatabaseRequest
}

func FromPb(impl *protobufs.DatabaseRequest) data.Request {
	return &Wrapper{
		impl: impl,
	}
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
