package store

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/data/request"
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

	// If no permissions are required, allow access
	if len(requiredPermissions) == 0 {
		return true
	}

	accessor := binding.NewEntity(ctx, me.store, me.accessorId)
	actualPermissions := accessor.GetField("Permissions").GetEntityList().GetEntities()

	// Also get total permissions which include those from roles
	totalPermissions := accessor.GetField("TotalPermissions").GetEntityList().GetEntities()
	if len(totalPermissions) > 0 {
		actualPermissions = append(actualPermissions, totalPermissions...)
	}

	// Check each required permission
	for _, requiredPermission := range requiredPermissions {
		if !me.hasPermission(ctx, requiredPermission, actualPermissions) {
			log.Debug("Permission (%s) not found for accessor (%s)", requiredPermission, me.accessorId)
			return false
		}
	}

	return true
}

// hasPermission checks if the user has the required permission or any parent permission
func (me *fieldAuthorizer) hasPermission(ctx context.Context, requiredPermission string, actualPermissions []string) bool {
	// Direct match check
	for _, actualPermission := range actualPermissions {
		if actualPermission == requiredPermission {
			return true
		}
	}

	// Check if any parent permission in the hierarchy is granted
	parentEntity := me.store.GetEntity(ctx, requiredPermission)
	if parentEntity == nil || parentEntity.GetType() != "Permission" {
		return false
	}

	// Get the parent permission
	parentReq := request.New().SetEntityId(requiredPermission).SetFieldName("Parent")
	me.store.Read(ctx, parentReq)

	if parentReq.IsSuccessful() && parentReq.GetValue().IsEntityReference() {
		parentId := parentReq.GetValue().GetEntityReference()
		if parentId != "" {
			// Check if user has permission for the parent
			return me.hasPermission(ctx, parentId, actualPermissions)
		}
	}

	return false
}
