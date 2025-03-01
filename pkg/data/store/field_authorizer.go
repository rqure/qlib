package store

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
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

func (me *fieldAuthorizer) IsAuthorized(ctx context.Context, entityId, fieldName string) bool {
	if entityId == "" || fieldName == "" {
		return false
	}

	// field := binding.NewField(&me.store, entityId, fieldName)
	// requiredAors := field.GetAreaOfResponsibility()
	// requiredPermissions := field.GetPermissions()

	// accessor := binding.NewEntity(ctx, me.store, me.accessorId)
	// actualAors := accessor.GetField("AreaOfResponsibilities").GetEntityList()
	// actualPermissions := accessor.GetField("Permissions").GetEntityList()
	return true
}
