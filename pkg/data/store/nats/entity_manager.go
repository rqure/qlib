package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/protobufs"
)

type EntityManager struct {
	core          Core
	schemaManager data.SchemaManager
	fieldOperator data.FieldOperator
}

func NewEntityManager(core Core) data.ModifiableEntityManager {
	return &EntityManager{core: core}
}

func (e *EntityManager) SetSchemaManager(sm data.SchemaManager) {
	e.schemaManager = sm
}

func (e *EntityManager) SetFieldOperator(fo data.FieldOperator) {
	e.fieldOperator = fo
}

func (e *EntityManager) CreateEntity(ctx context.Context, entityType, parentId, name string) {
	msg := &protobufs.ApiConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
		Name:     name,
	}

	e.core.Publish(e.core.GetKeyGenerator().GetReadSubject(), msg)
}

func (e *EntityManager) GetEntity(ctx context.Context, entityId string) data.Entity {
	msg := &protobufs.ApiConfigGetEntityRequest{
		Id: entityId,
	}

	resp, err := e.core.Request(ctx, e.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response protobufs.ApiConfigGetEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != protobufs.ApiConfigGetEntityResponse_SUCCESS {
		return nil
	}

	return entity.FromEntityPb(response.Entity)
}

func (e *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	msg := &protobufs.ApiConfigDeleteEntityRequest{
		Id: entityId,
	}

	e.core.Publish(e.core.GetKeyGenerator().GetReadSubject(), msg)
}

func (e *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	msg := &protobufs.ApiRuntimeGetEntitiesRequest{
		EntityType: entityType,
	}

	resp, err := e.core.Request(ctx, e.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response protobufs.ApiRuntimeGetEntitiesResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	ids := make([]string, len(response.Entities))
	for i, e := range response.Entities {
		ids[i] = e.Id
	}
	return ids
}

func (e *EntityManager) GetEntityTypes(ctx context.Context) []string {
	msg := &protobufs.ApiConfigGetEntityTypesRequest{}

	resp, err := e.core.Request(ctx, "entity.types", msg)
	if err != nil {
		return nil
	}

	var response protobufs.ApiConfigGetEntityTypesResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	return response.Types
}

func (e *EntityManager) SetEntity(ctx context.Context, entity data.Entity) {
	e.CreateEntity(ctx, entity.GetType(), entity.GetParentId(), entity.GetName())
}
