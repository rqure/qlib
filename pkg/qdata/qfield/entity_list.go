package qfield

import (
	"slices"

	"github.com/rqure/qlib/pkg/qdata"
)

// EntityListImpl implements the EntityList interface
type EntityListImpl struct {
	entities []string
}

// NewEntityList creates a new EntityList with the given entities
func NewEntityList(entities ...string) qdata.EntityList {
	return &EntityListImpl{
		entities: entities,
	}
}

func (e *EntityListImpl) GetEntities() []string {
	result := make([]string, e.Length())
	copy(result, e.getEntities())
	return result
}

func (e *EntityListImpl) SetEntities(entities []string) qdata.EntityList {
	e.setEntities(entities)
	return e
}

func (e *EntityListImpl) Add(entity string) bool {
	if e.Contains(entity) {
		return false
	}
	entities := e.getEntities()
	entities = append(entities, entity)
	e.setEntities(entities)
	return true
}

func (e *EntityListImpl) Remove(entity string) bool {
	if !e.Contains(entity) {
		return false
	}
	entities := e.getEntities()
	newEntities := slices.DeleteFunc(entities, func(e string) bool {
		return e == entity
	})
	e.setEntities(newEntities)
	return true
}

func (e *EntityListImpl) Contains(entity string) bool {
	return slices.Contains(e.entities, entity)
}

func (e *EntityListImpl) Count() int {
	return len(e.getEntities())
}

func (e *EntityListImpl) Clear() qdata.EntityList {
	e.setEntities([]string{})
	return e
}

func (e *EntityListImpl) At(index int) string {
	es := e.getEntities()
	return es[index]
}

func (e *EntityListImpl) SetAt(index int, entity string) qdata.EntityList {
	es := e.getEntities()
	es[index] = entity
	return e
}

func (e *EntityListImpl) Last() string {
	es := e.getEntities()
	return es[e.Length()-1]
}

func (e *EntityListImpl) First() string {
	es := e.getEntities()
	return es[0]
}

func (e *EntityListImpl) Length() int {
	return len(e.getEntities())
}

func (e *EntityListImpl) getEntities() []string {
	return e.entities
}

func (e *EntityListImpl) setEntities(entities []string) {
	e.entities = entities
}
