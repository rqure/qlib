package qdata

import (
	"context"
	"fmt"
)

type IndirectionResolver interface {
	Resolve(context.Context, EntityId, FieldType) (EntityId, FieldType, error)
}

type indirectionResolver struct {
	store StoreInteractor
}

func NewIndirectionResolver(store StoreInteractor) IndirectionResolver {
	return &indirectionResolver{
		store: store,
	}
}

// Examples or indirections
// 1. EntityReference1->EntityReference2->EntityReference3->Field
// 2. EntityList1->0->EntityReference1->Field
// 3. Parent->Field // Parent is an EntityReference type field
// 4. Children->0->Field // Children is an EntityList type field
func (me *indirectionResolver) Resolve(ctx context.Context, entityId EntityId, indirectFieldTypes FieldType) (EntityId, FieldType, error) {
	fields := indirectFieldTypes.AsIndirectionArray()

	if len(fields) == 1 {
		return entityId, indirectFieldTypes, nil
	}

	for i, f := range fields[:len(fields)-1] {
		// Handle array index navigation (for EntityList fields)
		if i > 0 && f.IsListIndex() {
			index := f.AsListIndex()
			if index < 0 {
				return "", "", fmt.Errorf("negative index: %d", index)
			}

			// The previous field should have been an EntityList
			// Its value is already loaded at this point, so we can just access it
			r := new(Request).Init(entityId, fields[i-1])
			me.store.Read(ctx, r)

			if !r.Success {
				return "", "", fmt.Errorf("failed to read entity list field: %v (%v)", fields[i-1], r.Err)
			}

			v := r.Value
			if !v.IsEntityList() {
				return "", "", fmt.Errorf("expected EntityList, got: %v", v)
			}

			entitiesList := v.GetEntityList()
			if index >= len(entitiesList) {
				return "", "", fmt.Errorf("array index out of bounds: %v >= %v", index, len(entitiesList))
			}

			entityId = entitiesList[index]
			continue
		}

		// Normal field resolution
		r := new(Request).Init(entityId, f)
		me.store.Read(ctx, r)

		if r.Success {
			v := r.Value

			// Handle EntityReference fields
			if v.IsEntityReference() {
				entityId = v.GetEntityReference()
				if entityId == "" {
					return "", "", fmt.Errorf("empty entity reference")
				}
				continue
			}

			// Handle EntityList fields (if not followed by an index)
			if v.IsEntityList() {
				// If next segment is not an index, this is an error
				if i+1 >= len(fields)-1 || !fields[i+1].IsListIndex() {
					return "", "", fmt.Errorf("expected index after EntityList, got: %v", fields[i+1])
				}
				// The index will be processed in the next iteration
				continue
			}

			return "", "", fmt.Errorf("field is not a reference type: %v", f)
		}

		// If we reach here, we couldn't resolve the field
		return "", "", fmt.Errorf("failed to resolve field: %v (%s)", f, r.Err)
	}

	return entityId, fields[len(fields)-1], nil
}
