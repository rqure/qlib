package qentity

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type Entity struct {
	impl *qprotobufs.DatabaseEntity
}

func ToEntityPb(e qdata.Entity) *qprotobufs.DatabaseEntity {
	if e == nil {
		return nil
	}

	return e.Impl().(*qprotobufs.DatabaseEntity)
}

func FromEntityPb(impl *qprotobufs.DatabaseEntity) qdata.Entity {
	return &Entity{
		impl: impl,
	}
}

func (e *Entity) GetId() string {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return e.impl.Id
}

func (e *Entity) GetType() string {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return e.impl.Type
}

func (e *Entity) SetId(id string) {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	e.impl.Id = id
}

func (e *Entity) SetType(t string) {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	e.impl.Type = t
}

func (e *Entity) Impl() any {
	if e.impl == nil {
		qlog.Error("Impl not defined")
	}

	return e.impl
}
