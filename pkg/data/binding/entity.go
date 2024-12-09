package binding

import "github.com/rqure/qlib/pkg/data"

type Entity struct {
	impl  data.Entity
	store data.Store
}

func NewEntity(store data.Store, entityId string) data.EntityBinding {
	e := store.GetEntity(entityId)

	return &Entity{
		store: store,
		impl:  e,
	}
}

func (e *Entity) GetId() string {
	return e.impl.GetId()
}

func (e *Entity) GetName() string {
	return e.impl.GetName()
}

func (e *Entity) GetType() string {
	return e.impl.GetType()
}

func (e *Entity) GetParentId() string {
	return e.impl.GetParentId()
}

func (e *Entity) GetChildrenIds() []string {
	return e.impl.GetChildrenIds()
}

func (e *Entity) AppendChildId(id string) {
	e.impl.AppendChildId(id)
}

func (e *Entity) RemoveChildId(id string) {
	e.impl.RemoveChildId(id)
}

func (e *Entity) SetChildrenIds(ids []string) {
	e.impl.SetChildrenIds(ids)
}

func (e *Entity) SetId(id string) {
	e.impl.SetId(id)
}

func (e *Entity) SetType(t string) {
	e.impl.SetType(t)
}

func (e *Entity) SetName(n string) {
	e.impl.SetName(n)
}

func (e *Entity) SetParentId(p string) {
	e.impl.SetParentId(p)
}

func (e *Entity) GetField(fieldName string) data.FieldBinding {
	return NewField(e.store, e.GetId(), fieldName)
}
