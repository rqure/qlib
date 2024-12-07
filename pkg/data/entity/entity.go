package entity

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Entity struct {
	impl *protobufs.DatabaseEntity
}

func ToEntityPb(e data.Entity) *protobufs.DatabaseEntity {
	if e == nil {
		return nil
	}

	switch c := e.(type) {
	case *Entity:
		return c.impl
	default:
		return nil
	}
}

func FromEntityPb(impl *protobufs.DatabaseEntity) data.Entity {
	return &Entity{
		impl: impl,
	}
}

func (e *Entity) GetId() string {
	return e.impl.Id
}

func (e *Entity) GetName() string {
	return e.impl.Name
}

func (e *Entity) GetType() string {
	return e.impl.Type
}

func (e *Entity) GetParentId() string {
	if e.impl.Parent == nil {
		return ""
	}

	return e.impl.Parent.Raw
}

func (e *Entity) GetChildrenIds() []string {
	ids := make([]string, len(e.impl.Children))
	for i, child := range e.impl.Children {
		ids[i] = child.Raw
	}

	return ids
}
