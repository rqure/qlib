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
	return NewEntityFromImpl(ctx, store, e)
}

func NewEntityFromImpl(ctx context.Context, store data.Store, impl data.Entity) data.EntityBinding {
	return &Entity{
		store:  store,
		impl:   impl,
		fields: make(map[string]data.FieldBinding),
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
		e.fields[fieldName] = NewField(&e.store, e.GetId(), fieldName)
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

func (e *Entity) DoMulti(ctx context.Context, fn func(data.EntityBinding)) {
	if e.impl == nil {
		log.Error("Impl not defined")
		return
	}

	// Temporarily replace the store with a multi-store
	store := e.store
	multi := NewMulti(store)
	e.store = multi

	// Perform the multi operation
	fn(e)
	multi.Commit(ctx)

	// Restore the original store
	e.store = store
}
