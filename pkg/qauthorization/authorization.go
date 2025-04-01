package qauthorization

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qscripting"
)

func HasPermission(ctx context.Context, accessorId qdata.EntityId, requiredPermissions []qdata.EntityId, store qdata.StoreInteractor) bool {
	accessor := new(qdata.Entity).Init(accessorId)

	permissions := []*qdata.Entity{}
	for _, permissionId := range requiredPermissions {
		permission := new(qdata.Entity).Init(permissionId)

		store.Read(ctx,
			permission.Field(qdata.FTPolicy).AsReadRequest())

		permissions = append(permissions, permission)
	}

	for _, permission := range permissions {
		scriptSrc := permission.Field(qdata.FTPolicy).Value.GetString()
		policy := qscripting.NewExecutor(scriptSrc)
		out, err := policy.Execute(ctx, map[string]qscripting.ObjectConverterFn{
			"ACCESSOR": qscripting.Entity(accessor),
			"STORE":    qscripting.Store(store),
		})
		if err != nil {
			qlog.Warn("Error executing script: %v", err)
			return false
		}

		if !out["ALLOW"].(bool) {
			return false
		}
	}

	return true
}

func CanRead(ctx context.Context, accessorId qdata.EntityId, resource *qdata.Field, store qdata.StoreInteractor) bool {
	resourceSchema := store.GetFieldSchema(ctx, resource.EntityId.GetEntityType(), resource.FieldType)
	if resourceSchema == nil {
		return false
	}

	return HasPermission(ctx, accessorId, resourceSchema.ReadPermissions, store)
}

func CanWrite(ctx context.Context, accessorId qdata.EntityId, resource *qdata.Field, store qdata.StoreInteractor) bool {
	resourceSchema := store.GetFieldSchema(ctx, resource.EntityId.GetEntityType(), resource.FieldType)
	if resourceSchema == nil {
		return false
	}

	return HasPermission(ctx, accessorId, resourceSchema.WritePermissions, store)
}

type Authorizer interface {
	AccessorId() qdata.EntityId
	CanRead(ctx context.Context, resource *qdata.Field) bool
	CanWrite(ctx context.Context, resource *qdata.Field) bool
}

type authorizer struct {
	accessorId qdata.EntityId
	store      qdata.StoreInteractor
}

func NewAuthorizer(accessorId qdata.EntityId, store qdata.StoreInteractor) Authorizer {
	return &authorizer{
		accessorId: accessorId,
		store:      store,
	}
}

func (me *authorizer) AccessorId() qdata.EntityId {
	return me.accessorId
}

func (me *authorizer) CanRead(ctx context.Context, resource *qdata.Field) bool {
	return CanRead(ctx, me.accessorId, resource, me.store)
}

func (me *authorizer) CanWrite(ctx context.Context, resource *qdata.Field) bool {
	return CanWrite(ctx, me.accessorId, resource, me.store)
}
