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

	return e.Impl().(*protobufs.DatabaseEntity)
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

func (e *Entity) AppendChildId(id string) {
	e.impl.Children = append(e.impl.Children, &protobufs.EntityReference{Raw: id})
}

func (e *Entity) RemoveChildId(id string) {
	for i, child := range e.impl.Children {
		if child.Raw == id {
			e.impl.Children = append(e.impl.Children[:i], e.impl.Children[i+1:]...)
			return
		}
	}
}

func (e *Entity) SetChildrenIds(ids []string) {
	e.impl.Children = make([]*protobufs.EntityReference, len(ids))
	for i, id := range ids {
		e.impl.Children[i] = &protobufs.EntityReference{Raw: id}
	}
}

func (e *Entity) SetId(id string) {
	e.impl.Id = id
}

func (e *Entity) SetName(name string) {
	e.impl.Name = name
}

func (e *Entity) SetType(t string) {
	e.impl.Type = t
}

func (e *Entity) SetParentId(id string) {
	if id == "" {
		e.impl.Parent = nil
		return
	}

	e.impl.Parent = &protobufs.EntityReference{Raw: id}
}

func (e *Entity) Impl() any {
	return e.impl
}
