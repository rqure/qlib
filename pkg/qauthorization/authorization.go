package qauthorization

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qscripting"
)

func HasPermission(ctx context.Context, subjectId qdata.EntityId, requiredPermissions []qdata.EntityId, store qdata.StoreInteractor) bool {
	subject := new(qdata.Entity).Init(subjectId)

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
			"SUBJECT": qscripting.Entity(subject),
			"STORE":   qscripting.Store(store),
		})
		if err != nil {
			qlog.Warn("Error executing script: %v", err)
			return false
		}

		allow, ok := out["ALLOW"]
		if !ok {
			return false
		}

		allowBool, ok := allow.(bool)
		if !ok {
			return false
		}

		if !allowBool {
			return false
		}
	}

	return true
}

func CanRead(ctx context.Context, subjectId qdata.EntityId, resource *qdata.Field, store qdata.StoreInteractor) bool {
	resourceSchema, err := store.GetFieldSchema(ctx, resource.EntityId.GetEntityType(), resource.FieldType)
	if err != nil {
		qlog.Warn("Failed to get field schema: %v", err)
		return false
	}

	if resourceSchema == nil {
		return false
	}

	return HasPermission(ctx, subjectId, resourceSchema.ReadPermissions, store) || HasKernelPermission(ctx, subjectId, store)
}

func CanWrite(ctx context.Context, subjectId qdata.EntityId, resource *qdata.Field, store qdata.StoreInteractor) bool {
	resourceSchema, err := store.GetFieldSchema(ctx, resource.EntityId.GetEntityType(), resource.FieldType)
	if err != nil {
		qlog.Warn("Failed to get field schema: %v", err)
		return false
	}

	if resourceSchema == nil {
		return false
	}

	return HasPermission(ctx, subjectId, resourceSchema.WritePermissions, store) || HasKernelPermission(ctx, subjectId, store)
}

func HasKernelPermission(ctx context.Context, subjectId qdata.EntityId, store qdata.StoreInteractor) bool {
	iter, err := store.PrepareQuery(`SELECT "$EntityId" FROM Permission WHERE Name = "Kernel"`, subjectId)
	if err != nil {
		qlog.Warn("Failed to prepare query: %v", err)
		return false
	}
	defer iter.Close()

	var permission *qdata.Entity
	iter.ForEach(ctx, func(row qdata.QueryRow) bool {
		permission = row.AsEntity()

		return false // Break after first permission
	})

	if permission == nil {
		qlog.Warn("kernel permission not found")
		return false
	}

	return HasPermission(ctx, subjectId, []qdata.EntityId{permission.EntityId}, store)
}

type Authorizer interface {
	SubjectId() qdata.EntityId
	CanRead(ctx context.Context, resource *qdata.Field) bool
	CanWrite(ctx context.Context, resource *qdata.Field) bool
}

type authorizer struct {
	subjectId qdata.EntityId
	store     qdata.StoreInteractor
}

func NewAuthorizer(subjectId qdata.EntityId, store qdata.StoreInteractor) Authorizer {
	return &authorizer{
		subjectId: subjectId,
		store:     store,
	}
}

func (me *authorizer) SubjectId() qdata.EntityId {
	return me.subjectId
}

func (me *authorizer) CanRead(ctx context.Context, resource *qdata.Field) bool {
	return CanRead(ctx, me.subjectId, resource, me.store)
}

func (me *authorizer) CanWrite(ctx context.Context, resource *qdata.Field) bool {
	return CanWrite(ctx, me.subjectId, resource, me.store)
}
