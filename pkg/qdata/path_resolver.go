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
	iterator, err := me.store.PrepareQuery(`SELECT "$EntityId", Children FROM Root WHERE Name = %q`, rootName)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %w", err)
	}
	defer iterator.Close()
	if !iterator.Next(ctx) {
		return nil, fmt.Errorf("failed to find root entity with name: %s", rootName)
	}

	// Now traverse the path
	currentEntity := iterator.Get().AsEntity()
	for _, name := range path[1:] {
		found := false
		// Find the child entity by name
		for _, childId := range currentEntity.Field("Children").Value.GetEntityList() {
			// Get the child entity
			childEntity := new(Entity).Init(childId)
			me.store.Read(ctx,
				childEntity.Field("Name").AsReadRequest(),
				childEntity.Field("Children").AsReadRequest(),
			)

			if childEntity.Field("Name").Value.GetString() == name {
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
