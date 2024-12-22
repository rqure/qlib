package binding

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
)

type Entity struct {
	impl   data.Entity
	store  data.Store
	fields map[string]data.FieldBinding
}

func NewEntity(ctx context.Context, store data.Store, entityId string) data.EntityBinding {
	e := store.GetEntity(ctx, entityId)

	return &Entity{
		store: store,
		impl:  e,
	}
}

func (e *Entity) GetId() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.GetId()
}

func (e *Entity) GetName() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.GetName()
}

func (e *Entity) GetType() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.GetType()
}

func (e *Entity) GetParentId() string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return ""
	}

	return e.impl.GetParentId()
}

func (e *Entity) GetChildrenIds() []string {
	if e.impl == nil {
		log.Error("Impl not defined")
		return []string{}
	}

	return e.impl.GetChildrenIds()
}

func (e *Entity) AppendChildId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.AppendChildId(id)
}

func (e *Entity) RemoveChildId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.RemoveChildId(id)
}

func (e *Entity) SetChildrenIds(ids []string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.SetChildrenIds(ids)
}

func (e *Entity) SetId(id string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.SetId(id)
}

func (e *Entity) SetType(t string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.SetType(t)
}

func (e *Entity) SetName(n string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.SetName(n)
}

func (e *Entity) SetParentId(p string) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	e.impl.SetParentId(p)
}

func (e *Entity) GetField(fieldName string) data.FieldBinding {
	if e.impl == nil {
		log.Error("Impl not defined")
		return nil
	}

	if e.fields[fieldName] == nil {
		e.fields[fieldName] = NewField(e.store, e.GetId(), fieldName)
	}

	return e.fields[fieldName]
}

func (e *Entity) Impl() any {
	if e.impl == nil {
		log.Error("Impl not defined")
		return nil
	}

	return e.impl.Impl()
}
