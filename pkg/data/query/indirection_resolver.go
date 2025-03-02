package query

import (
	"context"
	"strings"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
)

type IndirectionResolver struct {
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewIndirectionResolver(entityManager data.EntityManager, fieldOperator data.FieldOperator) data.IndirectionResolver {
	return &IndirectionResolver{entityManager: entityManager, fieldOperator: fieldOperator}
}

// Examples or indirections
// 1. EntityReference1->EntityReference2->EntityReference3->Field
// 2. EntityList1->0->EntityReference1->Field
// 3. Parent->Field // Parent is an EntityReference type field
// 4. Children->0->Field // Children is an EntityList type field
func (me *IndirectionResolver) Resolve(ctx context.Context, entityId, indirectField string) (string, string) {
	fields := strings.Split(indirectField, "->")

	if len(fields) == 1 {
		return entityId, indirectField
	}

	for i, f := range fields[:len(fields)-1] {
		// Handle array index navigation (for EntityList fields)
		if i > 0 && isArrayIndex(f) {
			index := parseArrayIndex(f)
			if index < 0 {
				log.Error("Invalid array index: %v", f)
				return "", ""
			}

			// The previous field should have been an EntityList
			// Its value is already loaded at this point, so we can just access it
			r := request.New().SetEntityId(entityId).SetFieldName(fields[i-1])
			me.fieldOperator.Read(ctx, r)

			if !r.IsSuccessful() {
				log.Error("Failed to read entity list field: %v", fields[i-1])
				return "", ""
			}

			v := r.GetValue()
			if !v.IsEntityList() {
				log.Error("Field is not an entity list: %v", fields[i-1])
				return "", ""
			}

			entitiesList := v.GetEntityList().GetEntities()
			if index >= len(entitiesList) {
				log.Error("Array index out of bounds: %v >= %v", index, len(entitiesList))
				return "", ""
			}

			entityId = entitiesList[index]
			continue
		}

		// Normal field resolution
		r := request.New().SetEntityId(entityId).SetFieldName(f)
		me.fieldOperator.Read(ctx, r)

		if r.IsSuccessful() {
			v := r.GetValue()

			// Handle EntityReference fields
			if v.IsEntityReference() {
				entityId = v.GetEntityReference()
				if entityId == "" {
					log.Error("Empty entity reference in field: %v", f)
					return "", ""
				}
				continue
			}

			// Handle EntityList fields (if not followed by an index)
			if v.IsEntityList() {
				// If next segment is not an index, this is an error
				if i+1 >= len(fields)-1 || !isArrayIndex(fields[i+1]) {
					log.Error("EntityList field not followed by index: %v", f)
					return "", ""
				}
				// The index will be processed in the next iteration
				continue
			}

			log.Error("Field is not a reference type (EntityReference or EntityList): %v", f)
			return "", ""
		}

		// If we can't resolve the field directly, try to find an entity by name
		// Look for a child entity with matching name
		childrenReq := request.New().SetEntityId(entityId).SetFieldName("Children")
		me.fieldOperator.Read(ctx, childrenReq)

		if childrenReq.IsSuccessful() && childrenReq.GetValue().IsEntityList() {
			childrenIds := childrenReq.GetValue().GetEntityList().GetEntities()
			foundChild := false

			for _, childId := range childrenIds {
				// Get the child's name
				nameReq := request.New().SetEntityId(childId).SetFieldName("Name")
				me.fieldOperator.Read(ctx, nameReq)

				if nameReq.IsSuccessful() && nameReq.GetValue().IsString() {
					childName := nameReq.GetValue().GetString()
					if childName == f {
						entityId = childId
						foundChild = true
						break
					}
				}
			}

			if foundChild {
				continue
			}
		}

		// Look for parent entity with matching name
		parentReq := request.New().SetEntityId(entityId).SetFieldName("Parent")
		me.fieldOperator.Read(ctx, parentReq)

		if parentReq.IsSuccessful() && parentReq.GetValue().IsEntityReference() {
			parentId := parentReq.GetValue().GetEntityReference()

			if parentId != "" {
				// Get the parent's name
				nameReq := request.New().SetEntityId(parentId).SetFieldName("Name")
				me.fieldOperator.Read(ctx, nameReq)

				if nameReq.IsSuccessful() && nameReq.GetValue().IsString() {
					parentName := nameReq.GetValue().GetString()
					if parentName == f {
						entityId = parentId
						continue
					}
				}
			}
		}

		// If we reach here, we couldn't resolve the field
		log.Error("Could not resolve field: %v for entity: %v", f, entityId)
		return "", ""
	}

	return entityId, fields[len(fields)-1]
}

// isArrayIndex checks if a string is an array index (digit)
func isArrayIndex(s string) bool {
	// Check if s is a non-negative integer
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0 // Must have at least one digit
}

// parseArrayIndex converts a string to an integer array index
func parseArrayIndex(s string) int {
	result := 0
	for _, c := range s {
		result = result*10 + int(c-'0')
	}
	return result
}
