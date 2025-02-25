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

func (me *IndirectionResolver) Resolve(ctx context.Context, entityId, indirectField string) (string, string) {
	fields := strings.Split(indirectField, "->")

	if len(fields) == 1 {
		return entityId, indirectField
	}

	for _, f := range fields[:len(fields)-1] {
		r := request.New().SetEntityId(entityId).SetFieldName(f)

		me.fieldOperator.Read(ctx, r)

		if r.IsSuccessful() {
			v := r.GetValue()
			if v.IsEntityReference() {
				entityId = v.GetEntityReference()

				if entityId == "" {
					log.Error("Failed to resolve entity reference: %v", r)
					return "", ""
				}

				continue
			}

			log.Error("Field is not an entity reference: %v", r)
			return "", ""
		}

		// Fallback to parent entity reference by name
		entity := me.entityManager.GetEntity(ctx, entityId)
		if entity == nil {
			log.Error("Failed to get entity: %v", entityId)
			return "", ""
		}

		parentId := entity.GetParentId()
		if parentId != "" {
			parentEntity := me.entityManager.GetEntity(ctx, parentId)

			if parentEntity != nil && parentEntity.GetName() == f {
				entityId = parentId
				continue
			}
		}

		// Fallback to child entity reference by name
		foundChild := false
		for _, childId := range entity.GetChildrenIds() {
			childEntity := me.entityManager.GetEntity(ctx, childId)
			if childEntity == nil {
				log.Error("Failed to get child entity: %v", childId)
				continue
			}

			if childEntity.GetName() == f {
				entityId = childId
				foundChild = true
				break
			}
		}

		if !foundChild {
			log.Error("Failed to find child entity: %v", f)
			return "", ""
		}
	}

	return entityId, fields[len(fields)-1]
}
