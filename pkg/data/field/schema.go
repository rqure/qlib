package field

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Schema struct {
	impl *protobufs.DatabaseFieldSchema
}

func FromSchemaPb(impl *protobufs.DatabaseFieldSchema) data.FieldSchema {
	return Schema{
		impl: impl,
	}
}

func (s Schema) GetFieldName() string {
	return s.impl.Name
}

func (s Schema) GetFieldType() string {
	return s.impl.Type
}
