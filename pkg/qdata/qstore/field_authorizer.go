package qstore

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

type fieldAuthorizer struct {
	accessorId qdata.EntityId
	store      qdata.Store
}

func NewFieldAuthorizer(accessorId qdata.EntityId, store qdata.Store) qdata.FieldAuthorizer {
	return &fieldAuthorizer{
		accessorId: accessorId,
		store:      store,
	}
}

func (me *fieldAuthorizer) AccessorId() qdata.EntityId {
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

	accessor := me.store.GetEntity(ctx, me.accessorId)

	// read the necessary fields
	me.store.Read(ctx,
		accessor.Field("Permissions").AsReadRequest(),
		accessor.Field("TotalPermissions").AsReadRequest(),
	)

	actualPermissions := accessor.Field("Permissions").Value.GetEntityList()
	// Also get total permissions which include those from other roles that are not currently active
	totalPermissions := accessor.Field("TotalPermissions").Value.GetEntityList()
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
