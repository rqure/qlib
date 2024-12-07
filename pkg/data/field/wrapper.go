package field

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Wrapper struct {
	impl *protobufs.DatabaseField
}

func FromPb(impl *protobufs.DatabaseField) data.Field {
	return &Wrapper{
		impl: impl,
	}
}

func (f *Wrapper) GetValue() data.Value {
	return FromAnyPb(f.impl.Value)
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
