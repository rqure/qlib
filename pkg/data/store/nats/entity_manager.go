package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/log"
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

func (me *EntityManager) SetSchemaManager(sm data.SchemaManager) {
	me.schemaManager = sm
}

func (me *EntityManager) SetFieldOperator(fo data.FieldOperator) {
	me.fieldOperator = fo
}

func (me *EntityManager) CreateEntity(ctx context.Context, entityType, parentId, name string) string {
	msg := &protobufs.ApiConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
		Name:     name,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		log.Error("Failed to create entity: %v", err)
	}

	var response protobufs.ApiConfigCreateEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		log.Error("Failed to create entity: %v", err)
	}

	if response.Status != protobufs.ApiConfigCreateEntityResponse_SUCCESS {
		log.Error("Failed to create entity: %v", response.Status)
	}

	return response.Id
}

func (me *EntityManager) GetEntity(ctx context.Context, entityId string) data.Entity {
	msg := &protobufs.ApiConfigGetEntityRequest{
		Id: entityId,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
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

func (me *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	msg := &protobufs.ApiConfigDeleteEntityRequest{
		Id: entityId,
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		log.Error("Failed to delete entity: %v", err)
	}
}

func (me *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	msg := &protobufs.ApiRuntimeGetEntitiesRequest{
		EntityType: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
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

func (me *EntityManager) GetEntityTypes(ctx context.Context) []string {
	msg := &protobufs.ApiConfigGetEntityTypesRequest{}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response protobufs.ApiConfigGetEntityTypesResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	return response.Types
}

func (me *EntityManager) EntityExists(ctx context.Context, entityId string) bool {
	msg := &protobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response protobufs.ApiRuntimeEntityExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}
