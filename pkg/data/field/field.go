package field

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Field struct {
	impl *protobufs.DatabaseField
}

func FromFieldPb(impl *protobufs.DatabaseField) data.Field {
	return &Field{
		impl: impl,
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
