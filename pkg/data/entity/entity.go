package entity

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type Entity struct {
	impl *protobufs.DatabaseEntity
}

func ToEntityPb(e data.Entity) *protobufs.DatabaseEntity {
	if e == nil {
		return nil
	}

	return e.Impl().(*protobufs.DatabaseEntity)
}

func FromEntityPb(impl *protobufs.DatabaseEntity) data.Entity {
	return &Entity{
		impl: impl,
	}
}

func (e *Entity) GetId() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.Id
}

func (e *Entity) GetType() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.Type
}

func (e *Entity) SetId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Id = id
}

func (e *Entity) SetType(t string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Type = t
}

func (e *Entity) Impl() any {
	if e.impl == nil {
		log.Error("Impl not defined")
	}

	return e.impl
}
