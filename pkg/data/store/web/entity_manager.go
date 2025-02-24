package web

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
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
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
		Name:     name,
	})

	e.core.SendAndWait(ctx, msg)
}

func (e *EntityManager) GetEntity(ctx context.Context, entityId string) data.Entity {
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiConfigGetEntityRequest{
		Id: entityId,
	})

	response := e.core.SendAndWait(ctx, msg)
	if response == nil {
		return nil
	}

	var resp protobufs.ApiConfigGetEntityResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	if resp.Status != protobufs.ApiConfigGetEntityResponse_SUCCESS {
		return nil
	}

	return entity.FromEntityPb(resp.Entity)
}

func (e *EntityManager) SetEntity(ctx context.Context, entity data.Entity) {
	// Create entity is used instead of set entity
	e.CreateEntity(ctx, entity.GetType(), entity.GetParentId(), entity.GetName())
}

func (e *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiConfigDeleteEntityRequest{
		Id: entityId,
	})

	e.core.SendAndWait(ctx, msg)
}

func (e *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiRuntimeGetEntitiesRequest{
		EntityType: entityType,
	})

	response := e.core.SendAndWait(ctx, msg)
	if response == nil {
		return nil
	}

	var resp protobufs.ApiRuntimeGetEntitiesResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	ids := make([]string, len(resp.Entities))
	for i, e := range resp.Entities {
		ids[i] = e.Id
	}
	return ids
}

func (e *EntityManager) GetEntityTypes(ctx context.Context) []string {
	msg := web.NewMessage()
	msg.Header = &protobufs.ApiHeader{}
	msg.Payload, _ = anypb.New(&protobufs.ApiConfigGetEntityTypesRequest{})

	response := e.core.SendAndWait(ctx, msg)
	if response == nil {
		return nil
	}

	var resp protobufs.ApiConfigGetEntityTypesResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return nil
	}

	return resp.Types
}
