package qstore

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
	"github.com/rqure/qlib/pkg/qlog"
)

type fieldAuthorizer struct {
	accessorId string
	store      qdata.Store
}

func NewFieldAuthorizer(accessorId string, store qdata.Store) qdata.FieldAuthorizer {
	return &fieldAuthorizer{
		accessorId: accessorId,
		store:      store,
	}
}

func (me *fieldAuthorizer) AccessorId() string {
	return me.accessorId
}

func (me *fieldAuthorizer) IsAuthorized(ctx context.Context, entityId qdata.EntityId, fieldType qdata.FieldType, forWrite bool) bool {
	if entityId == "" || fieldType == "" {
		qlog.Error("Invalid entityId (%s) or fieldName (%s)", entityId, fieldType)
		return false
	}

	entity := me.store.GetEntity(ctx, entityId)
	if entity == nil {
		qlog.Error("Entity not found for entityId (%s)", entityId)
		return false
	}

	fieldSchema := me.store.GetFieldSchema(ctx, entity.EntityType, fieldType)
	if fieldSchema == nil {
		qlog.Error("Field schema not found for entityId (%s) and fieldName (%s)", entityId, fieldType)
		return false
	}

	var requiredPermissions []qdata.EntityId
	if forWrite {
		requiredPermissions = fieldSchema.WritePermissions
	} else {
		requiredPermissions = fieldSchema.ReadPermissions
	}

	// If no permissions are required, allow access
	if len(requiredPermissions) == 0 {
		return true
	}

	accessor := qbinding.NewEntity(ctx, me.store, me.accessorId)
	actualPermissions := accessor.GetField("Permissions").GetEntityList().GetEntities()

	// Also get total permissions which include those from roles
	totalPermissions := accessor.GetField("TotalPermissions").GetEntityList().GetEntities()
	if len(totalPermissions) > 0 {
		actualPermissions = append(actualPermissions, totalPermissions...)
	}

	// Check each required permission
	for _, requiredPermission := range requiredPermissions {
		if !me.hasPermission(ctx, requiredPermission, actualPermissions) {
			qlog.Debug("Permission (%s) not found for accessor (%s)", requiredPermission, me.accessorId)
			return false
		}
	}

	return true
}

// hasPermission checks if the user has the required permission or any parent permission
func (me *fieldAuthorizer) hasPermission(ctx context.Context, requiredPermission qdata.EntityId, actualPermissions []qdata.EntityId) bool {
	// Direct match check
	for _, actualPermission := range actualPermissions {
		if actualPermission == requiredPermission {
			return true
		}
	}

	// Check if any parent permission in the hierarchy is granted
	parentEntity := me.store.GetEntity(ctx, requiredPermission)
	if parentEntity == nil || parentEntity.EntityType != qdata.ETPermission {
		return false
	}

	// Get the parent permission
	parentReq := new(qdata.Request).Init(requiredPermission, qdata.FTParent)
	me.store.Read(ctx, parentReq)

	if parentReq.Success && parentReq.Value.IsEntityReference() {
		parentId := parentReq.Value.GetEntityReference()
		if !parentId.IsEmpty() {
			// Check if user has permission for the parent
			return me.hasPermission(ctx, parentId, actualPermissions)
		}
	}

	return false
}
