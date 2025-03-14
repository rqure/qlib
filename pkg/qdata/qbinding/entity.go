package qbinding

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

type Entity struct {
	impl   qdata.Entity
	store  qdata.Store
	fields map[string]qdata.FieldBinding
}

func NewEntity(ctx context.Context, store qdata.Store, entityId string) qdata.EntityBinding {
	e := store.GetEntity(ctx, entityId)
	return NewEntityFromImpl(ctx, store, e)
}

func NewEntityFromImpl(ctx context.Context, store qdata.Store, impl qdata.Entity) qdata.EntityBinding {
	return &Entity{
		store:  store,
		impl:   impl,
		fields: make(map[string]qdata.FieldBinding),
	}
}

func (e *Entity) GetId() string {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return e.impl.GetId()
}

func (e *Entity) GetType() string {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return ""
	}

	return e.impl.GetType()
}

func (e *Entity) SetId(id string) {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	e.impl.SetId(id)
}

func (e *Entity) SetType(t string) {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return
	}

	e.impl.SetType(t)
}

func (e *Entity) GetField(fieldName string) qdata.FieldBinding {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return nil
	}

	if e.fields[fieldName] == nil {
		e.fields[fieldName] = NewField(&e.store, e.GetId(), fieldName)
	}

	return e.fields[fieldName]
}

func (e *Entity) Impl() any {
	if e.impl == nil {
		qlog.Error("Impl not defined")
		return nil
	}

	return e.impl.Impl()
}

func (e *Entity) DoMulti(ctx context.Context, fn func(qdata.EntityBinding)) {
	if e.impl == nil {
		qlog.Error("Impl not defined")
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
