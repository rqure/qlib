package field

import (
	"slices"

	"github.com/rqure/qlib/pkg/data"
)

// EntityListImpl implements the EntityList interface
type EntityListImpl struct {
	entities []string
}

// NewEntityList creates a new EntityList with the given entities
func NewEntityList(entities []string) data.EntityList {
	return &EntityListImpl{
		entities: entities,
	}
}

func (e *EntityListImpl) GetEntities() []string {
	result := make([]string, len(e.entities))
	copy(result, e.entities)
	return result
}

func (e *EntityListImpl) SetEntities(entities []string) data.EntityList {
	e.entities = entities
	return e
}

func (e *EntityListImpl) Add(entity string) bool {
	if e.Contains(entity) {
		return false
	}
	e.entities = append(e.entities, entity)
	return true
}

func (e *EntityListImpl) Remove(entity string) bool {
	for i, id := range e.entities {
		if id == entity {
			// Remove by swapping with last element and truncating
			e.entities[i] = e.entities[len(e.entities)-1]
			e.entities = e.entities[:len(e.entities)-1]
			return true
		}
	}
	return false
}

func (e *EntityListImpl) Contains(entity string) bool {
	return slices.Contains(e.entities, entity)
}

func (e *EntityListImpl) Count() int {
	return len(e.entities)
}

func (e *EntityListImpl) Clear() data.EntityList {
	e.entities = []string{}
	return e
}
