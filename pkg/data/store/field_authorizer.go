package store

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/log"
)

type FieldAuthorizer interface {
	data.FieldAuthorizer
}

type fieldAuthorizer struct {
	accessorId string
	store      data.Store
}

func NewFieldAuthorizer(accessorId string, store data.Store) FieldAuthorizer {
	return &fieldAuthorizer{
		accessorId: accessorId,
		store:      store,
	}
}

func (me *fieldAuthorizer) AccessorId() string {
	return me.accessorId
}

func (me *fieldAuthorizer) IsAuthorized(ctx context.Context, entityId, fieldName string, forWrite bool) bool {
	if entityId == "" || fieldName == "" {
		log.Error("Invalid entityId (%s) or fieldName (%s)", entityId, fieldName)
		return false
	}

	fieldSchema := me.store.GetFieldSchema(ctx, entityId, fieldName)
	if fieldSchema == nil {
		log.Error("Field schema not found for entityId (%s) and fieldName (%s)", entityId, fieldName)
		return false
	}

	var requiredPermissions []string
	if forWrite {
		requiredPermissions = fieldSchema.GetWritePermissions()
	} else {
		requiredPermissions = fieldSchema.GetReadPermissions()
	}

	accessor := binding.NewEntity(ctx, me.store, me.accessorId)
	actualPermissions := accessor.GetField("Permissions").GetEntityList().GetEntities()

	for _, requiredPermission := range requiredPermissions {
		found := false
		for _, actualPermission := range actualPermissions {
			if actualPermission == requiredPermission {
				found = true
				break
			}
		}
		if !found {
			log.Debug("Permission (%s) not found for accessor (%s)", requiredPermission, me.accessorId)
			return false
		}
	}

	return true
}
