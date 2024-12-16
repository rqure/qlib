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

func (e *Entity) GetName() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.Name
}

func (e *Entity) GetType() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.Type
}

func (e *Entity) GetParentId() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	if e.impl.Parent == nil {
		return ""
	}

	return e.impl.Parent.Raw
}

func (e *Entity) GetChildrenIds() []string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return []string{}
	}

	ids := make([]string, len(e.impl.Children))
	for i, child := range e.impl.Children {
		ids[i] = child.Raw
	}

	return ids
}

func (e *Entity) AppendChildId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Children = append(e.impl.Children, &protobufs.EntityReference{Raw: id})
}

func (e *Entity) RemoveChildId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	for i, child := range e.impl.Children {
		if child.Raw == id {
			e.impl.Children = append(e.impl.Children[:i], e.impl.Children[i+1:]...)
			return
		}
	}
}

func (e *Entity) SetChildrenIds(ids []string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Children = make([]*protobufs.EntityReference, len(ids))
	for i, id := range ids {
		e.impl.Children[i] = &protobufs.EntityReference{Raw: id}
	}
}

func (e *Entity) SetId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Id = id
}

func (e *Entity) SetName(name string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Name = name
}

func (e *Entity) SetType(t string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.Type = t
}

func (e *Entity) SetParentId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	if id == "" {
		e.impl.Parent = nil
		return
	}

	e.impl.Parent = &protobufs.EntityReference{Raw: id}
}

func (e *Entity) Impl() any {
	if e.impl == nil {
		log.Error("Impl not defined")
	}

	return e.impl
}
