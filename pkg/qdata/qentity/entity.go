package qentity

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type Entity struct {
	impl *qprotobufs.DatabaseEntity
}

func ToEntityPb(e qdata.Entity) *qprotobufs.DatabaseEntity {
	if e == nil {
		return nil
	}

	if e, ok := e.(*Entity); ok {
		return e.impl
	}

	return nil
}

func FromEntityPb(impl *qprotobufs.DatabaseEntity) qdata.Entity {
	return &Entity{
		impl: impl,
	}
}

func (e *Entity) GetId() string {
	return e.impl.Id
}

func (e *Entity) GetType() string {
	return e.impl.Type
}

func (e *Entity) SetId(id string) {
	e.impl.Id = id
}

func (e *Entity) SetType(t string) {
	e.impl.Type = t
}
