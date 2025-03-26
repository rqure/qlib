package qdata

import (
	"context"

	"github.com/rqure/qlib/pkg/qlog"
)

type indirectionResolver struct {
	store StoreInteractor
}

func NewIndirectionResolver(store StoreInteractor) IndirectionResolver {
	return &indirectionResolver{}
}

// Examples or indirections
// 1. EntityReference1->EntityReference2->EntityReference3->Field
// 2. EntityList1->0->EntityReference1->Field
// 3. Parent->Field // Parent is an EntityReference type field
// 4. Children->0->Field // Children is an EntityList type field
func (me *indirectionResolver) Resolve(ctx context.Context, entityId EntityId, indirectFieldTypes FieldType) (EntityId, FieldType) {
	fields := indirectFieldTypes.AsIndirectionArray()

	if len(fields) == 1 {
		return entityId, indirectFieldTypes
	}

	for i, f := range fields[:len(fields)-1] {
		// Handle array index navigation (for EntityList fields)
		if i > 0 && f.IsListIndex() {
			index := f.AsListIndex()
			if index < 0 {
				qlog.Error("Invalid array index: %v", f)
				return "", ""
			}

			// The previous field should have been an EntityList
			// Its value is already loaded at this point, so we can just access it
			r := new(Request).Init(entityId, fields[i-1])
			me.store.Read(ctx, r)

			if !r.Success {
				qlog.Error("Failed to read entity list field: %v", fields[i-1])
				return "", ""
			}

			v := r.Value
			if !v.IsEntityList() {
				qlog.Error("Field is not an entity list: %v", fields[i-1])
				return "", ""
			}

			entitiesList := v.GetEntityList()
			if index >= len(entitiesList) {
				qlog.Error("Array index out of bounds: %v >= %v", index, len(entitiesList))
				return "", ""
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
					qlog.Error("Empty entity reference in field: %v", f)
					return "", ""
				}
				continue
			}

			// Handle EntityList fields (if not followed by an index)
			if v.IsEntityList() {
				// If next segment is not an index, this is an error
				if i+1 >= len(fields)-1 || !fields[i+1].IsListIndex() {
					qlog.Error("EntityList field not followed by index: %v", f)
					return "", ""
				}
				// The index will be processed in the next iteration
				continue
			}

			qlog.Error("Field is not a reference type (EntityReference or EntityList): %v", f)
			return "", ""
		}

		// If we reach here, we couldn't resolve the field
		qlog.Error("Could not resolve field: %v for entity: %v", f, entityId)
		return "", ""
	}

	return entityId, fields[len(fields)-1]
}
