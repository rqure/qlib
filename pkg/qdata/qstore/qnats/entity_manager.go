package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type EntityManager struct {
	core          Core
	schemaManager qdata.SchemaManager
	fieldOperator qdata.FieldOperator
}

func NewEntityManager(core Core) qdata.ModifiableEntityManager {
	return &EntityManager{core: core}
}

func (me *EntityManager) SetSchemaManager(sm qdata.SchemaManager) {
	me.schemaManager = sm
}

func (me *EntityManager) SetFieldOperator(fo qdata.FieldOperator) {
	me.fieldOperator = fo
}

func (me *EntityManager) CreateEntity(ctx context.Context, entityType, parentId, name string) string {
	msg := &qprotobufs.ApiConfigCreateEntityRequest{
		Type:     entityType,
		ParentId: parentId,
		Name:     name,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to create entity: %v", err)
	}

	var response qprotobufs.ApiConfigCreateEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		qlog.Error("Failed to create entity: %v", err)
	}

	if response.Status != qprotobufs.ApiConfigCreateEntityResponse_SUCCESS {
		qlog.Error("Failed to create entity: %v", response.Status)
	}

	return response.Id
}

func (me *EntityManager) GetEntity(ctx context.Context, entityId string) qdata.Entity {
	msg := &qprotobufs.ApiConfigGetEntityRequest{
		Id: entityId,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigGetEntityResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	if response.Status != qprotobufs.ApiConfigGetEntityResponse_SUCCESS {
		return nil
	}

	return qentity.FromEntityPb(response.Entity)
}

func (me *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	msg := &qprotobufs.ApiConfigDeleteEntityRequest{
		Id: entityId,
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetWriteSubject(), msg)
	if err != nil {
		qlog.Error("Failed to delete entity: %v", err)
	}
}

func (me *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	msg := &qprotobufs.ApiRuntimeGetEntitiesRequest{
		EntityType: entityType,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiRuntimeGetEntitiesResponse
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
	msg := &qprotobufs.ApiConfigGetEntityTypesRequest{}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return nil
	}

	var response qprotobufs.ApiConfigGetEntityTypesResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return nil
	}

	return response.Types
}

func (me *EntityManager) EntityExists(ctx context.Context, entityId string) bool {
	msg := &qprotobufs.ApiRuntimeEntityExistsRequest{
		EntityId: entityId,
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetReadSubject(), msg)
	if err != nil {
		return false
	}

	var response qprotobufs.ApiRuntimeEntityExistsResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return false
	}

	return response.Exists
}

type paginatedResult struct {
	em          *EntityManager
	ctx         context.Context
	entityType  string
	pageSize    int
	totalCount  int
	currentPage int
	entities    []string
	current     int
	lastErr     error
}

func (p *paginatedResult) Next(ctx context.Context) bool {
	// If we've reached the end of current page, fetch next page
	if p.entities == nil || p.current >= len(p.entities) {
		msg := &qprotobufs.ApiRuntimeGetEntitiesPaginatedRequest{
			EntityType: p.entityType,
			Page:       int32(p.currentPage),
			PageSize:   int32(p.pageSize),
		}

		resp, err := p.em.core.Request(ctx, p.em.core.GetKeyGenerator().GetReadSubject(), msg)
		if err != nil {
			p.lastErr = err
			return false
		}

		var response qprotobufs.ApiRuntimeGetEntitiesPaginatedResponse
		if err := resp.Payload.UnmarshalTo(&response); err != nil {
			p.lastErr = err
			return false
		}

		if len(response.Entities) == 0 {
			return false
		}

		p.entities = make([]string, len(response.Entities))
		for i, e := range response.Entities {
			p.entities[i] = e.Id
		}
		p.totalCount = int(response.TotalCount)
		p.current = 0
		p.currentPage++
	}

	p.current++
	return true
}

func (p *paginatedResult) Value() string {
	if p.current > 0 && p.current <= len(p.entities) {
		return p.entities[p.current-1]
	}
	return ""
}

func (p *paginatedResult) Error() error {
	return p.lastErr
}

func (p *paginatedResult) TotalCount() int {
	return p.totalCount
}

func (me *EntityManager) FindEntitiesPaginated(ctx context.Context, entityType string, page, pageSize int) qdata.FindEntitiesPaginatedResult {
	return &paginatedResult{
		em:          me,
		ctx:         ctx,
		entityType:  entityType,
		pageSize:    pageSize,
		currentPage: page,
	}
}
