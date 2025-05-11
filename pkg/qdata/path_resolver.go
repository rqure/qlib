package qdata

import (
	"context"
	"fmt"
)

// import (
// 	"context"
// )

// /* Example database structure:

// Root
// │
// └── Security Models
//     │
//     ├── Permissions
//     │   └── System
//     │       ├── Security
//     │       ├── Configuration
//     │       └── Application
//     │
//     ├── Areas of Responsibility
//     │   └── System
//     │       └── Database
//     │
//     ├── Roles
//     │   └── Admin
//     │
//     ├── Users
//     │   └── qei
//     │
//     ├── Clients
//     │   └── qcore

// All Entities have a Parent (EntityReference), Children (EntityList), and Name field for hierarchical navigation.

// The path ["Root", "Security Models", "Permissions", "System"] would resolve to the Entity with name "System" under the Permissions entity.

// Note that the path resolver goes by name, not ID. Underneath, the path resolver finds the EntityId while resolving the path.

// */

type PathResolver interface {
	Resolve(context.Context, ...string) (*Entity, error)
}

type pathResolver struct {
	store StoreInteractor
}

func NewPathResolver(store StoreInteractor) PathResolver {
	return &pathResolver{
		store: store,
	}
}

func (me *pathResolver) Resolve(ctx context.Context, path ...string) (*Entity, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path cannot be empty")
	}

	// Start with finding the root entity by name
	rootName := path[0]
	roots, err := me.store.Find(ctx,
		ETRoot,
		[]FieldType{FTName, FTChildren},
		func(e *Entity) bool { return e.Field(FTName).Value.GetString() == rootName })
	if err != nil {
		return nil, fmt.Errorf("failed to find root entity: %w", err)
	}

	var currentEntity *Entity
	for _, root := range roots {
		currentEntity = root
	}
	if currentEntity == nil {
		return nil, fmt.Errorf("failed to find root entity with name: %s", rootName)
	}

	for _, name := range path[1:] {
		found := false
		// Find the child entity by name
		for _, childId := range currentEntity.Field(FTChildren).Value.GetEntityList() {
			// Get the child entity
			childEntity := new(Entity).Init(childId)
			me.store.Read(ctx,
				childEntity.Field(FTName).AsReadRequest(),
				childEntity.Field(FTChildren).AsReadRequest(),
			)

			if childEntity.Field(FTName).Value.GetString() == name {
				currentEntity = childEntity
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("failed to find entity with name: %s", name)
		}
	}

	return currentEntity, nil
}
