package qpostgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
)

type PostgresStoreInteractor struct {
	core         PostgresCore
	publisherSig qss.Signal[qdata.PublishNotificationArgs]
	clientId     *qdata.EntityId

	// Cache support
	cache        Cache
	cacheTTL     time.Duration
	keyBuilder   *CacheKeyBuilder
	cacheEnabled bool
}

// NewStoreInteractor creates a new store interactor with optional cache support
func NewStoreInteractor(core PostgresCore, cache Cache, cacheTTL time.Duration) *PostgresStoreInteractor {
	cacheEnabled := cache != nil
	keyBuilder := NewCacheKeyBuilder("qdata")

	if cacheEnabled {
		qlog.Info("PostgreSQL cache enabled with TTL: %v", cacheTTL)
	}

	return &PostgresStoreInteractor{
		core:         core,
		publisherSig: qss.New[qdata.PublishNotificationArgs](),
		cache:        cache,
		cacheTTL:     cacheTTL,
		keyBuilder:   keyBuilder,
		cacheEnabled: cacheEnabled,
	}
}

// getFromCache attempts to get a value from cache
func (me *PostgresStoreInteractor) getFromCache(ctx context.Context, key string) ([]byte, bool) {
	if !me.cacheEnabled {
		return nil, false
	}
	return me.cache.Get(ctx, key)
}

// setInCache stores a value in cache with the configured TTL
func (me *PostgresStoreInteractor) setInCache(ctx context.Context, key string, value []byte) {
	if !me.cacheEnabled {
		return
	}
	if err := me.cache.Set(ctx, key, value, me.cacheTTL); err != nil {
		qlog.Debug("Failed to cache value for key %s: %v", key, err)
	}
}

// invalidateFromCache removes a value from cache
func (me *PostgresStoreInteractor) invalidateFromCache(ctx context.Context, key string) {
	if !me.cacheEnabled {
		return
	}
	if err := me.cache.Delete(ctx, key); err != nil {
		qlog.Debug("Failed to invalidate cache for key %s: %v", key, err)
	}
}

func (me *PostgresStoreInteractor) GetEntity(ctx context.Context, entityId qdata.EntityId) *qdata.Entity {
	// Try cache first
	cacheKey := me.keyBuilder.ForEntity(entityId)
	if cachedData, found := me.getFromCache(ctx, cacheKey); found {
		if entity, err := DeserializeEntity(cachedData); err == nil {
			return entity
		}
	}

	var result *qdata.Entity

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// First get the entity's basic info
		var entityType string

		err := tx.QueryRow(ctx, `
		SELECT type
		FROM Entities
		WHERE id = $1
		`, entityId.AsString()).Scan(&entityType)

		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				qlog.Error("Failed to get entity: %v", err)
			}
			return
		}

		result = new(qdata.Entity).Init(qdata.EntityId(entityId))
	})

	// Cache the result if found
	if result != nil {
		if data, err := SerializeEntity(result); err == nil {
			me.setInCache(ctx, cacheKey, data)
		}
	}

	return result
}

func (me *PostgresStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) qdata.EntityId {
	entityIdGenerator := func(entityType qdata.EntityType) qdata.EntityId {
		return qdata.EntityId(fmt.Sprintf("%s$%s", entityType.AsString(), uuid.New().String()))
	}

	entityId := entityIdGenerator(entityType)

	for me.EntityExists(ctx, entityId) {
		entityId = entityIdGenerator(entityType)
	}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `
            INSERT INTO Entities (id, type)
            VALUES ($1, $2)
        `, entityId.AsString(), entityType.AsString())

		if err != nil {
			qlog.Error("Failed to create entity: %v", err)
			entityId = ""
			return
		}

		// Initialize fields with default values
		schema := me.GetEntitySchema(ctx, entityType)
		if schema != nil {
			reqs := []*qdata.Request{}
			for _, f := range schema.Fields {
				req := new(qdata.Request).Init(entityId, f.FieldType)

				if f.FieldType == qdata.FTName {
					req.Value.FromString(name)
				} else if f.FieldType == qdata.FTParent {
					req.Value.FromEntityReference(parentId)
				}

				reqs = append(reqs, req)
			}
			me.Write(ctx, reqs...)
		}

		// Only update parent's children if parentId is provided
		if parentId != "" {
			req := new(qdata.Request).Init(parentId, qdata.FTChildren)
			me.Read(ctx, req)
			if req.Success {
				children := req.Value.GetEntityList()
				children = append(children, entityId)
				req.Value.SetEntityList(children)
				me.Write(ctx, req)
			}
		}
	})

	// Invalidate any parent's cache entry if needed
	if parentId != "" {
		me.invalidateFromCache(ctx, me.keyBuilder.ForEntity(parentId))
		me.invalidateFromCache(ctx, me.keyBuilder.ForField(parentId, qdata.FTChildren))
	}

	return entityId
}

func (me *PostgresStoreInteractor) FindEntities(entityType qdata.EntityType, pageOpts ...qdata.PageOpts) *qdata.PageResult[qdata.EntityId] {
	// We don't cache paginated results since they're already optimized by the database
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityId]{
		Items:    []qdata.EntityId{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
			var entities []qdata.EntityId
			var nextCursorId int64 = pageConfig.CursorId

			me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				// Request exact page size
				rows, err := tx.Query(ctx, `
                    SELECT id, cursor_id 
                    FROM Entities 
                    WHERE type = $1 AND cursor_id > $2 
                    ORDER BY cursor_id 
                    LIMIT $3
                `, entityType.AsString(), pageConfig.CursorId, pageConfig.PageSize)

				if err != nil {
					qlog.Error("Failed to find entities: %v", err)
					return
				}
				defer rows.Close()

				for rows.Next() {
					var id string
					var cursorId int64
					if err := rows.Scan(&id, &cursorId); err != nil {
						qlog.Error("Failed to scan entity: %v", err)
						continue
					}
					entities = append(entities, qdata.EntityId(id))
					nextCursorId = cursorId // Keep track of the last cursor ID
				}

				// If we got fewer items than requested, we're at the end
				if len(entities) < int(pageConfig.PageSize) {
					nextCursorId = -1
				}
			})

			return &qdata.PageResult[qdata.EntityId]{
				Items:    entities,
				CursorId: nextCursorId,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityId], error) {
					if nextCursorId < 0 {
						return &qdata.PageResult[qdata.EntityId]{
							Items:    []qdata.EntityId{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					return me.FindEntities(entityType,
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursorId)).NextPage(ctx)
				},
			}, nil
		},
	}
}

func (me *PostgresStoreInteractor) GetEntityTypes(pageOpts ...qdata.PageOpts) *qdata.PageResult[qdata.EntityType] {
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	return &qdata.PageResult[qdata.EntityType]{
		Items:    []qdata.EntityType{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
			var types []qdata.EntityType
			var nextCursorId int64 = pageConfig.CursorId

			me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				rows, err := tx.Query(ctx, `
					SELECT DISTINCT ON (entity_type) entity_type, cursor_id
					FROM EntitySchema
					WHERE cursor_id > $1
					ORDER BY entity_type, cursor_id
					LIMIT $2
                `, pageConfig.CursorId, pageConfig.PageSize)

				if err != nil {
					qlog.Error("Failed to get entity types: %v", err)
					return
				}
				defer rows.Close()

				for rows.Next() {
					var entityType string
					var cursorId int64
					if err := rows.Scan(&entityType, &cursorId); err != nil {
						qlog.Error("Failed to scan entity type: %v", err)
						continue
					}
					types = append(types, qdata.EntityType(entityType))
					nextCursorId = cursorId // Keep track of the last cursor ID
				}

				// If we got fewer items than requested, we're at the end
				if len(types) < int(pageConfig.PageSize) {
					nextCursorId = -1
				}
			})

			return &qdata.PageResult[qdata.EntityType]{
				Items:    types,
				CursorId: nextCursorId,
				NextPage: func(ctx context.Context) (*qdata.PageResult[qdata.EntityType], error) {
					if nextCursorId < 0 {
						return &qdata.PageResult[qdata.EntityType]{
							Items:    []qdata.EntityType{},
							CursorId: -1,
							NextPage: nil,
						}, nil
					}
					return me.GetEntityTypes(
						qdata.POPageSize(pageConfig.PageSize),
						qdata.POCursorId(nextCursorId)).NextPage(ctx)
				},
			}, nil
		},
	}
}

func (me *PostgresStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) {
	// Collect all entities to delete in the correct order (children before parents)
	entitiesToDelete := me.collectDeletionOrderIterative(ctx, entityId)

	// Before deleting, invalidate cache for all entities
	for _, id := range entitiesToDelete {
		me.invalidateEntityFromCache(ctx, id)
	}

	// Delete entities in the correct order (children first)
	for _, id := range entitiesToDelete {
		me.deleteEntityWithoutChildren(ctx, id)
	}
}

// invalidateEntityFromCache invalidates all cache entries for an entity
func (me *PostgresStoreInteractor) invalidateEntityFromCache(ctx context.Context, entityId qdata.EntityId) {
	if !me.cacheEnabled {
		return
	}

	// Invalidate entity itself
	me.invalidateFromCache(ctx, me.keyBuilder.ForEntity(entityId))

	// Get the entity to access its type
	entity := me.GetEntity(ctx, entityId)
	if entity == nil {
		return
	}

	// Get schema to learn about fields
	schema := me.GetEntitySchema(ctx, entity.EntityType)
	if schema == nil {
		return
	}

	// Invalidate all fields
	for fieldType := range schema.Fields {
		me.invalidateFromCache(ctx, me.keyBuilder.ForField(entityId, fieldType))
	}
}

// collectDeletionOrderIterative builds a list of entities to delete in the right order (children before parents)
// using an iterative depth-first traversal approach
func (me *PostgresStoreInteractor) collectDeletionOrderIterative(ctx context.Context, rootEntityId qdata.EntityId) []qdata.EntityId {
	var result []qdata.EntityId

	// We need two data structures:
	// 1. A stack for DFS traversal
	// 2. A visited map to track which entities we've already processed
	type stackItem struct {
		id        string
		processed bool // Whether we've already processed children
	}

	stack := []stackItem{{id: rootEntityId.AsString(), processed: false}}
	visited := make(map[qdata.EntityId]bool)

	for len(stack) > 0 {
		// Get the top item from stack
		current := stack[len(stack)-1]

		if current.processed {
			// If we've already processed children, add to result and pop from stack
			stack = stack[:len(stack)-1]
			if !visited[qdata.EntityId(current.id)] {
				result = append(result, qdata.EntityId(current.id))
				visited[qdata.EntityId(current.id)] = true
			}
		} else {
			// Mark as processed and get children
			stack[len(stack)-1].processed = true

			childrenReq := new(qdata.Request).Init(qdata.EntityId(current.id), qdata.FTChildren)
			me.Read(ctx, childrenReq)

			if childrenReq.Success {
				children := childrenReq.Value.GetEntityList()

				// Add children to stack in reverse order (so we process in original order)
				for i := len(children) - 1; i >= 0; i-- {
					childId := children[i]
					// Only add if not already visited
					if !visited[childId] {
						stack = append(stack, stackItem{id: childId.AsString(), processed: false})
					}
				}
			}
		}
	}

	return result
}

// deleteEntityWithoutChildren deletes a single entity, handling its references but not its children
func (me *PostgresStoreInteractor) deleteEntityWithoutChildren(ctx context.Context, entityId qdata.EntityId) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Check if entity exists
		entity := me.GetEntity(ctx, entityId)
		if entity == nil {
			qlog.Error("Entity %s does not exist", entityId)
			return
		}

		type Ref struct {
			ByEntityId  string
			ByFieldType string
		}

		// Remove references to this entity from other entities
		err := BatchedQuery(me.core, ctx, `
            SELECT referenced_by_entity_id, referenced_by_field_type, cursor_id 
            FROM ReverseEntityReferences 
            WHERE referenced_entity_id = $1
        `,
			[]any{entityId.AsString()},
			0,
			func(rows pgx.Rows, cursorId *int64) (Ref, error) {
				var ref Ref
				err := rows.Scan(&ref.ByEntityId, &ref.ByFieldType, cursorId)
				return ref, err
			},
			func(batch []Ref) error {
				for _, ref := range batch {

					// Read the current value
					req := new(qdata.Request).Init(qdata.EntityId(ref.ByEntityId), qdata.FieldType(ref.ByFieldType))
					me.Read(ctx, req)

					if !req.Success {
						return fmt.Errorf("failed to read field %s for entity %s", ref.ByFieldType, ref.ByEntityId)
					}

					// Update the reference based on its type
					if req.Value.IsEntityReference() {
						// If it's a direct reference, clear it
						if req.Value.GetEntityReference() == entityId {
							req.Value.SetEntityReference("")
							me.Write(ctx, req)
						}
					} else if req.Value.IsEntityList() {
						// If it's a list of references, remove this entity from the list
						entities := req.Value.GetEntityList()
						updatedEntities := []qdata.EntityId{}

						for _, id := range entities {
							if id != entityId {
								updatedEntities = append(updatedEntities, id)
							}
						}

						req.Value.SetEntityList(updatedEntities)
						me.Write(ctx, req)
					}
				}
				return nil
			},
		)

		if err != nil {
			qlog.Error("Failed to query reverse references: %v", err)
		} else {
			// Clean up the reverse references table
			_, err = tx.Exec(ctx, `
                DELETE FROM ReverseEntityReferences WHERE referenced_entity_id = $1
                OR referenced_by_entity_id = $1
            `, entityId.AsString())
			if err != nil {
				qlog.Error("Failed to delete reverse references: %v", err)
			}
		}

		if entity.EntityType == qdata.ETPermission {
			// Remove permissions from schemas
			_, err := tx.Exec(ctx, `
				UPDATE EntitySchema 
				SET read_permissions = array_remove(read_permissions, $1),
					write_permissions = array_remove(write_permissions, $1)
				WHERE $1 = ANY(read_permissions) OR $1 = ANY(write_permissions)
			`, entityId.AsString())

			if err != nil {
				qlog.Error("Failed to remove permission from schemas: %v", err)
			}
		}

		// Delete all field values
		for _, valueType := range qdata.ValueTypes {
			tableName := getTableNameForType(valueType)
			_, err := tx.Exec(ctx, fmt.Sprintf(`
                DELETE FROM %s WHERE entity_id = $1
            `, tableName), entityId)
			if err != nil {
				qlog.Error("Failed to delete fields from %s: %v", tableName, err)
				return
			}
		}

		// Finally delete the entity itself
		_, err = tx.Exec(ctx, `
            DELETE FROM Entities WHERE id = $1
        `, entityId)
		if err != nil {
			qlog.Error("Failed to delete entity: %v", err)
			return
		}
	})
}

func (me *PostgresStoreInteractor) EntityExists(ctx context.Context, entityId qdata.EntityId) bool {
	// Try cache first for faster existence check
	if me.cacheEnabled {
		if _, found := me.getFromCache(ctx, me.keyBuilder.ForEntity(entityId)); found {
			return true
		}
	}

	exists := false

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM Entities WHERE id = $1)
		`, entityId.AsString()).Scan(&exists)

		if err != nil {
			qlog.Error("Failed to check entity existence: %v", err)
		}
	})

	return exists
}

func (me *PostgresStoreInteractor) Read(ctx context.Context, requests ...*qdata.Request) {
	ir := qdata.NewIndirectionResolver(me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			req.Success = false

			indirectEntity, indirectField := ir.Resolve(ctx, req.EntityId, req.FieldType)
			if indirectField == "" || indirectEntity == "" {
				qlog.Error("Failed to resolve indirection for: %s->%s", req.EntityId, req.FieldType)
				continue
			}

			// Check if field data is in cache
			cacheKey := me.keyBuilder.ForField(indirectEntity, indirectField)
			if cachedData, found := me.getFromCache(ctx, cacheKey); found {
				entityId, fieldType, value, writeTime, writerId, err := DeserializeFieldData(cachedData)
				if err == nil && entityId == indirectEntity && fieldType == indirectField {
					req.Value.FromValue(value)
					req.WriteTime.FromTime(writeTime.AsTime())
					req.WriterId.FromString(writerId.AsString())
					req.Success = true
					continue
				}
			}

			entity := me.GetEntity(ctx, indirectEntity)
			if entity == nil {
				qlog.Error("Failed to get entity: %s", indirectEntity)
				continue
			}

			schema := me.GetFieldSchema(ctx, entity.EntityType, indirectField)
			if schema == nil {
				qlog.Error("Failed to get field schema: %s->%s", entity.EntityType, indirectField)
				continue
			}

			tableName := getTableNameForType(schema.ValueType)
			if tableName == "" {
				qlog.Error("Invalid value_type '%s' for field %s->%s", schema.ValueType, entity.EntityType, indirectField)
				continue
			}

			if authorizer, ok := qcontext.GetAuthorizer(ctx); ok {
				if !authorizer.CanRead(ctx, new(qdata.Field).Init(indirectEntity, indirectField)) {
					qlog.Warn("%s is not authorized to read from field: %s->%s", authorizer.AccessorId(), req.EntityId, req.FieldType)
					continue
				}
			}

			fieldValue := schema.ValueType.NewValue().GetRaw()
			var writeTime time.Time
			var writer string
			err := tx.QueryRow(ctx, fmt.Sprintf(`
					SELECT field_value, write_time, writer
					FROM %s
					WHERE entity_id = $1 AND field_type = $2
				`, tableName), indirectEntity.AsString(), indirectField.AsString()).Scan(&fieldValue, &writeTime, &writer)

			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					qlog.Error("Failed to read field: %v", err)
				}
				continue
			}

			value := schema.ValueType.NewValue(fieldValue)
			if value == nil {
				qlog.Error("Failed to convert value for field %s->%s", entity.EntityType, indirectField)
				continue
			}

			req.Value.FromValue(value)
			req.WriteTime.FromTime(writeTime)
			req.WriterId.FromString(writer)
			req.Success = true

			// Cache the field data
			if me.cacheEnabled {
				fieldData, err := SerializeFieldData(indirectEntity, indirectField, value, *req.WriteTime, *req.WriterId)
				if err == nil {
					me.setInCache(ctx, cacheKey, fieldData)
				}
			}
		}
	})
}

func (me *PostgresStoreInteractor) Write(ctx context.Context, requests ...*qdata.Request) {
	ir := qdata.NewIndirectionResolver(me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			indirectEntity, indirectField := ir.Resolve(ctx, req.EntityId, req.FieldType)
			if indirectField == "" || indirectEntity == "" {
				qlog.Error("Failed to resolve indirection for: %s->%s", req.EntityId, req.FieldType)
				continue
			}

			// Get entity and schema info in a single query
			var entityType, fieldType, valueType string
			err := tx.QueryRow(ctx, `
				WITH entity_type AS (
					SELECT type FROM Entities WHERE id = $1
				)
				SELECT 
					entity_type.type,
					EntitySchema.field_type,
					EntitySchema.value_type
				FROM entity_type
				LEFT JOIN EntitySchema ON 
					EntitySchema.entity_type = entity_type.type
					AND EntitySchema.field_type = $2
			`, indirectEntity.AsString(), indirectField.AsString()).Scan(&entityType, &fieldType, &valueType)
			schema := new(qdata.FieldSchema).Init(qdata.EntityType(entityType), qdata.FieldType(fieldType), qdata.ValueType(valueType))

			if err != nil {
				qlog.Error("Failed to get entity and schema info: %v", err)
				continue
			}

			tableName := getTableNameForType(schema.ValueType)
			if tableName == "" {
				qlog.Error("Invalid value_type '%s' for field %s->%s", valueType, entityType, indirectField)
				continue
			}

			if req.Value.IsNil() {
				req.Value.FromValue(schema.ValueType.NewValue())
			}

			oldReq := new(qdata.Request).Init(req.EntityId, req.FieldType)
			me.Read(ctx, oldReq)

			if oldReq.Success && req.WriteOpt == qdata.WriteChanges {
				if oldReq.Value.Equals(req.Value) {
					req.Success = true
					continue
				}
			}

			fieldValue := req.Value.GetRaw()
			if fieldValue == nil {
				qlog.Error("Failed to convert value for field %s->%s", entityType, indirectField)
				continue
			}

			if req.WriteTime == nil {
				wt := time.Now()
				req.WriteTime = new(qdata.WriteTime).FromTime(wt)
			}

			if req.WriterId == nil || req.WriterId.IsEmpty() {
				wr := new(qdata.EntityId).FromString("")

				appName := qcontext.GetAppName(ctx)
				if me.clientId == nil && appName != "" {
					me.PrepareQuery("SELECT Name FROM Client WHERE Name = %q", appName).
						ForEach(ctx, func(client *qdata.Entity) bool {
							me.clientId = &client.EntityId
							return false
						})
				}

				if me.clientId != nil {
					*wr = *me.clientId
				}

				req.WriterId = wr
			}

			if authorizer, ok := qcontext.GetAuthorizer(ctx); ok {
				if !authorizer.CanWrite(ctx, new(qdata.Field).Init(indirectEntity, indirectField)) {
					qlog.Error("%s is not authorized to write to field: %s->%s", authorizer.AccessorId(), req.EntityId, req.FieldType)
					continue
				}
			}

			if oldReq.Success && (oldReq.Value.IsEntityReference() || oldReq.Value.IsEntityList()) {
				var oldReferences []qdata.EntityId
				if oldReq.Value.IsEntityReference() {
					oldRef := oldReq.Value.GetEntityReference()
					if !oldRef.IsEmpty() {
						oldReferences = []qdata.EntityId{oldRef}
					}
				} else if oldReq.Value.IsEntityList() {
					oldReferences = oldReq.Value.GetEntityList()
				}

				// Delete old references
				for _, oldRef := range oldReferences {
					_, err = tx.Exec(ctx, `
                            DELETE FROM ReverseEntityReferences 
                            WHERE referenced_entity_id = $1 
                            AND referenced_by_entity_id = $2
                            AND referenced_by_field_type = $3
                        `, oldRef.AsString(), indirectEntity.AsString(), indirectField.AsString())

					if err != nil {
						qlog.Error("Failed to delete old reverse entity reference: %v", err)
					}

					// Invalidate the referenced entity's cache as well
					me.invalidateEntityFromCache(ctx, oldRef)
				}
			}

			if req.Value.IsEntityReference() || req.Value.IsEntityList() {
				var newReferences []qdata.EntityId
				if req.Value.IsEntityReference() {
					newRef := req.Value.GetEntityReference()
					if !newRef.IsEmpty() && me.EntityExists(ctx, newRef) {
						newReferences = []qdata.EntityId{newRef}
						req.Value.SetEntityReference(newRef)
					} else {
						req.Value.SetEntityReference("")
					}
				} else if req.Value.IsEntityList() {
					for _, newRef := range req.Value.GetEntityList() {
						if !newRef.IsEmpty() && me.EntityExists(ctx, newRef) {
							newReferences = append(newReferences, newRef)
						}
					}
					req.Value.SetEntityList(newReferences)
				}

				// Insert new references
				for _, newRef := range newReferences {
					_, err = tx.Exec(ctx, `
                        INSERT INTO ReverseEntityReferences 
                        (referenced_entity_id, referenced_by_entity_id, referenced_by_field_type)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (referenced_entity_id, referenced_by_entity_id, referenced_by_field_type) 
                        DO NOTHING
                    `, newRef.AsString(), indirectEntity.AsString(), indirectField.AsString())

					if err != nil {
						qlog.Error("Failed to insert reverse entity reference: %v", err)
					}

					// Invalidate the referenced entity's cache as well
					me.invalidateEntityFromCache(ctx, newRef)
				}
			}

			// Upsert the field value
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (entity_id, field_type, field_value, write_time, writer)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (entity_id, field_type) 
				DO UPDATE SET field_value = $3, write_time = $4, writer = $5
			`, tableName), indirectEntity.AsString(), indirectField.AsString(), fieldValue, req.WriteTime.AsTime(), req.WriterId.AsString())

			if err != nil {
				qlog.Error("Failed to write field: %v", err)
				continue
			}

			// Invalidate the field's cache
			me.invalidateFromCache(ctx, me.keyBuilder.ForField(indirectEntity, indirectField))

			// If this field update affects entity references, invalidate relevant entity caches
			if req.Value.IsEntityReference() || req.Value.IsEntityList() {
				me.invalidateEntityFromCache(ctx, indirectEntity)
			}

			// Handle notifications
			me.publisherSig.Emit(qdata.PublishNotificationArgs{
				Ctx:  ctx,
				Curr: req,
				Prev: oldReq,
			})

			req.Success = true
		}
	})
}

func (me *PostgresStoreInteractor) InitializeSchema(ctx context.Context) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Check if core tables exist
		var exists bool
		err := tx.QueryRow(ctx, `
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'entities'
            )
        `).Scan(&exists)

		if err != nil {
			qlog.Error("Failed to check if tables exist: %v", err)
			return
		}

		if !exists {
			// Tables don't exist, initialize the database
			_, err = tx.Exec(ctx, createTablesSQL)
			if err != nil {
				qlog.Error("Failed to create tables: %v", err)
				return
			}

			_, err = tx.Exec(ctx, createIndexesSQL)
			if err != nil {
				qlog.Error("Failed to create indexes: %v", err)
				return
			}

			qlog.Info("Database structure initialized successfully")
		}
	})
}

func (me *PostgresStoreInteractor) RestoreSnapshot(ctx context.Context, ss *qdata.Snapshot) {
	// If we're restoring, clear any cache first
	if me.cacheEnabled {
		if err := me.cache.Flush(ctx); err != nil {
			qlog.Warn("Failed to clear cache before snapshot restore: %v", err)
		}
	}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// First drop indexes explicitly
		_, err := tx.Exec(ctx, `
				-- Drop all indexes on entity tables
				DROP INDEX IF EXISTS idx_entities_type;
				
				-- Drop EntitySchema indexes
				DROP INDEX IF EXISTS idx_entityschema_entity_type;
				DROP INDEX IF EXISTS idx_entityschema_permissions;
				
				-- Drop field value table indexes
				DROP INDEX IF EXISTS idx_strings_entity_id;
				DROP INDEX IF EXISTS idx_binaryfiles_entity_id;
				DROP INDEX IF EXISTS idx_ints_entity_id;
				DROP INDEX IF EXISTS idx_floats_entity_id;
				DROP INDEX IF EXISTS idx_bools_entity_id;
				DROP INDEX IF EXISTS idx_entityreferences_entity_id;
				DROP INDEX IF EXISTS idx_timestamps_entity_id;
				DROP INDEX IF EXISTS idx_choices_entity_id;
				DROP INDEX IF EXISTS idx_entitylists_entity_id;
				
				-- Drop reference tracking indexes
				DROP INDEX IF EXISTS idx_entityreferences_field_value;
				DROP INDEX IF EXISTS idx_entitylists_field_value;
				
				-- Drop reverse references indexes
				DROP INDEX IF EXISTS idx_reverseentityreferences_referenced_entity_id;
				DROP INDEX IF EXISTS idx_reverseentityreferences_referenced_by_entity_id;
				DROP INDEX IF EXISTS idx_reverseentityreferences_referenced_by_field_type;
				
				-- Drop write time indexes
				DROP INDEX IF EXISTS idx_strings_write_time;
				DROP INDEX IF EXISTS idx_entityreferences_write_time;
				DROP INDEX IF EXISTS idx_timestamps_write_time;
				DROP INDEX IF EXISTS idx_timestamps_field_value;
				
				-- Drop choice option indexes
				DROP INDEX IF EXISTS idx_choiceoptions_entity_type;
			`)

		if err != nil {
			qlog.Error("Failed to drop indexes: %v", err)
			// Continue anyway since we'll drop tables next
		}

		// Remove existing tables
		_, err = tx.Exec(ctx, `
            DROP TABLE IF EXISTS
                Entities, EntitySchema, Strings,
                BinaryFiles, Ints, Floats, Bools, EntityReferences,
                Timestamps, Choices, ChoiceOptions, EntityLists,
                ReverseEntityReferences
            CASCADE
        `)

		if err != nil {
			qlog.Error("Failed to clear existing data: %v", err)
			return
		}

		// Recreate tables and indexes
		if err := me.initializeDatabase(ctx); err != nil {
			qlog.Error("Failed to initialize database: %v", err)
			return
		}

		for _, schema := range ss.Schemas {
			me.SetEntitySchema(ctx, schema)
		}

		// Restore entities
		for _, e := range ss.Entities {
			_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, type)
			VALUES ($1, $2)
		`, e.EntityId.AsString(), e.EntityType.AsString())
			if err != nil {
				qlog.Error("Failed to restore entity: %v", err)
				continue
			}
		}

		// Restore fields
		for _, f := range ss.Fields {
			me.Write(ctx, f.AsWriteRequest())
		}

		// Restore schemas again because permissions are missed in the first pass
		for _, schema := range ss.Schemas {
			me.SetEntitySchema(ctx, schema)
		}
	})
}

func (me *PostgresStoreInteractor) CreateSnapshot(ctx context.Context) *qdata.Snapshot {
	ss := new(qdata.Snapshot).Init()

	// Get all entity types and their schemas
	me.GetEntityTypes().ForEach(ctx, func(entityType qdata.EntityType) bool {
		// Add schema
		schema := me.GetEntitySchema(ctx, entityType)
		if schema == nil {
			qlog.Warn("Failed to get schema for entity type %s", entityType)
			return true
		}

		ss.Schemas = append(ss.Schemas, schema)

		// Add entitiesIterator of this type and their fields
		me.FindEntities(entityType).ForEach(ctx, func(entityId qdata.EntityId) bool {
			entity := me.GetEntity(ctx, entityId)
			if entity == nil {
				qlog.Warn("Failed to get entity %s", entityId)
				return true
			}

			ss.Entities = append(ss.Entities, entity)

			// Add fields for this entity
			for fieldType := range schema.Fields {
				req := entity.Field(fieldType).AsReadRequest()
				me.Read(ctx, req)
				if req.Success {
					ss.Fields = append(ss.Fields, entity.Field(fieldType))
				}
			}

			return true
		})
		return true
	})

	return ss
}

func (me *PostgresStoreInteractor) initializeDatabase(ctx context.Context) error {
	var err error
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		_, err = tx.Exec(ctx, createTablesSQL)
		if err != nil {
			err = fmt.Errorf("failed to create tables: %v", err)
			return
		}

		_, err = tx.Exec(ctx, createIndexesSQL)
		if err != nil {
			err = fmt.Errorf("failed to create indexes: %v", err)
			return
		}
	})
	return err
}

func (me *PostgresStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) *qdata.FieldSchema {
	// Try cache first
	cacheKey := me.keyBuilder.ForFieldSchema(entityType, fieldType)
	if cachedData, found := me.getFromCache(ctx, cacheKey); found {
		if schema, err := DeserializeFieldSchema(cachedData); err == nil {
			return schema
		}
	}

	schemaPb := &qprotobufs.DatabaseFieldSchema{}
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
            SELECT field_type, value_type, read_permissions, write_permissions
            FROM EntitySchema
            WHERE entity_type = $1 AND field_type = $2
        `, entityType, fieldType).Scan(&schemaPb.Name, &schemaPb.Type, &schemaPb.ReadPermissions, &schemaPb.WritePermissions)
		if err != nil {
			qlog.Error("Failed to get field schema: %v", err)
			schemaPb = nil
		}
	})

	if schemaPb == nil {
		return nil
	}

	schema := new(qdata.FieldSchema).FromFieldSchemaPb(entityType, schemaPb)
	if schema.ValueType == qdata.VTChoice {
		var options []string
		me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
			err := tx.QueryRow(ctx, `
				SELECT options
				FROM ChoiceOptions
				WHERE entity_type = $1 AND field_type = $2
			`, entityType, fieldType).Scan(&options)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					qlog.Error("Failed to get choice options: %v", err)
				}
			}
		})

		schema.Choices = options
	}

	// Cache the schema
	if me.cacheEnabled && schema != nil {
		if schemaData, err := SerializeFieldSchema(schema); err == nil {
			me.setInCache(ctx, cacheKey, schemaData)
		}
	}

	return schema
}

func (me *PostgresStoreInteractor) SetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType, schema *qdata.FieldSchema) {
	// Find entity schema
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		qlog.Error("Failed to get entity schema for type %s", entityType)
		return
	}

	entitySchema.Fields[fieldType] = schema

	me.SetEntitySchema(ctx, entitySchema)

	// Invalidate schema caches
	me.invalidateFromCache(ctx, me.keyBuilder.ForEntitySchema(entityType))
	me.invalidateFromCache(ctx, me.keyBuilder.ForFieldSchema(entityType, fieldType))
}

func (me *PostgresStoreInteractor) FieldExists(ctx context.Context, entityType qdata.EntityType, fieldType qdata.FieldType) bool {
	// Try cache first for faster existence check
	cacheKey := me.keyBuilder.ForFieldSchema(entityType, fieldType)
	if me.cacheEnabled {
		if _, found := me.getFromCache(ctx, cacheKey); found {
			return true
		}
	}

	exists := false

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM EntitySchema 
				WHERE entity_type = $1 AND field_type = $2
			)
		`, entityType, fieldType).Scan(&exists)
		if err != nil {
			qlog.Error("Failed to check field existence: %v", err)
		}
	})

	return exists
}

func (me *PostgresStoreInteractor) SetEntitySchema(ctx context.Context, requestedSchema *qdata.EntitySchema) {
	// Invalidate schema caches before updating
	if me.cacheEnabled {
		me.invalidateFromCache(ctx, me.keyBuilder.ForEntitySchema(requestedSchema.EntityType))
		for fieldType := range requestedSchema.Fields {
			me.invalidateFromCache(ctx, me.keyBuilder.ForFieldSchema(requestedSchema.EntityType, fieldType))
		}
	}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get existing schema for comparison
		oldSchema := me.GetEntitySchema(ctx, requestedSchema.EntityType)

		// Delete existing schema
		_, err := tx.Exec(ctx, `
			DELETE FROM EntitySchema WHERE entity_type = $1
		`, requestedSchema.EntityType.AsString())
		if err != nil {
			qlog.Error("Failed to delete existing schema: %v", err)
			return
		}

		// Delete existing choice options
		_, err = tx.Exec(ctx, `
			DELETE FROM ChoiceOptions WHERE entity_type = $1
		`, requestedSchema.EntityType.AsString())
		if err != nil {
			qlog.Error("Failed to delete existing choice options: %v", err)
			return
		}

		// Build new schema
		requestedSchema.Field(qdata.FTName, qdata.FSOValueType(qdata.VTString))
		requestedSchema.Field(qdata.FTDescription, qdata.FSOValueType(qdata.VTString))
		requestedSchema.Field(qdata.FTParent, qdata.FSOValueType(qdata.VTEntityReference))
		requestedSchema.Field(qdata.FTChildren, qdata.FSOValueType(qdata.VTEntityList))

		for _, fieldSchema := range requestedSchema.Fields {
			// Remove non-existant entity ids from read/write permissions
			readPermissions := []string{}
			for _, id := range fieldSchema.ReadPermissions {
				entity := me.GetEntity(ctx, id)
				if entity != nil && entity.EntityType == qdata.ETPermission {
					readPermissions = append(readPermissions, id.AsString())
				}
			}

			writePermissions := []string{}
			for _, id := range fieldSchema.WritePermissions {
				entity := me.GetEntity(ctx, id)
				if entity != nil && entity.EntityType == qdata.ETPermission {
					writePermissions = append(writePermissions, id.AsString())
				}
			}

			_, err = tx.Exec(ctx, `
            INSERT INTO EntitySchema (entity_type, field_type, value_type, read_permissions, write_permissions, rank)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (entity_type, field_type) 
            DO UPDATE SET value_type = $3, read_permissions = $4, write_permissions = $5, rank = $6
        `, requestedSchema.EntityType.AsString(), fieldSchema.FieldType.AsString(), fieldSchema.ValueType.AsString(), readPermissions, writePermissions, fieldSchema.Rank)

			if err != nil {
				qlog.Error("Failed to set field schema: %v", err)
				return
			}

			// Handle choice options if this is a choice field
			if fieldSchema.ValueType == qdata.VTChoice {
				_, err = tx.Exec(ctx, `
                INSERT INTO ChoiceOptions (entity_type, field_type, options)
                VALUES ($1, $2, $3)
                ON CONFLICT (entity_type, field_type)
                DO UPDATE SET options = $3
            `, requestedSchema.EntityType.AsString(), fieldSchema.FieldType.AsString(), fieldSchema.Choices)

				if err != nil {
					qlog.Error("Failed to set choice options: %v", err)
					return
				}
			}
		}

		// Handle field changes for existing entities
		if oldSchema != nil {
			removedFields := []qdata.FieldType{}
			newFields := []qdata.FieldType{}

			// Find removed fields
			for _, oldField := range oldSchema.Fields {
				found := false
				for _, newField := range requestedSchema.Fields {
					if oldField.FieldType == newField.FieldType {
						found = true
						break
					}
				}
				if !found {
					removedFields = append(removedFields, oldField.FieldType)
				}
			}

			// Find new fields
			for _, newField := range requestedSchema.Fields {
				found := false
				for _, oldField := range oldSchema.Fields {
					if newField.FieldType == oldField.FieldType {
						found = true
						break
					}
				}
				if !found {
					newFields = append(newFields, newField.FieldType)
				}
			}

			// Update existing entitiesIterator
			me.FindEntities(requestedSchema.EntityType).ForEach(ctx, func(entityId qdata.EntityId) bool {
				// Remove deleted fields
				for _, fieldType := range removedFields {
					tableName := getTableNameForType(oldSchema.Fields[fieldType].ValueType)
					if tableName == "" {
						continue
					}
					_, err = tx.Exec(ctx, fmt.Sprintf(`
						DELETE FROM %s 
						WHERE entity_id = $1 AND field_type = $2
					`, tableName), entityId, fieldType)
					if err != nil {
						qlog.Error("Failed to delete field: %v", err)
						continue
					}
				}

				// Initialize new fields
				for _, fieldType := range newFields {
					req := new(qdata.Request).Init(entityId, fieldType)
					me.Write(ctx, req)
				}

				return true
			})
		}
	})
}

func (me *PostgresStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) *qdata.EntitySchema {
	// Try cache first
	// cacheKey := me.keyBuilder.ForEntitySchema(entityType)
	// if _, found := me.getFromCache(ctx, cacheKey); found {
	// For entity schemas, we don't have a direct deserialization function
	// since it's a more complex object. Instead, we'll rely on
	// individual field schema caching for performance.
	// }

	schema := new(qdata.EntitySchema).Init(entityType)

	type FieldRow struct {
		FieldType        string
		ValueType        string
		Rank             int
		ReadPermissions  []string
		WritePermissions []string
	}

	var fieldRows []FieldRow

	err := BatchedQuery(me.core, ctx, `
		SELECT field_type, value_type, read_permissions, write_permissions, rank, cursor_id
		FROM EntitySchema
		WHERE entity_type = $1
	`,
		[]any{entityType.AsString()},
		0, // use default batch size
		func(rows pgx.Rows, cursorId *int64) (FieldRow, error) {
			var fr FieldRow
			err := rows.Scan(&fr.FieldType, &fr.ValueType, &fr.ReadPermissions, &fr.WritePermissions, &fr.Rank, cursorId)
			return fr, err
		},
		func(batch []FieldRow) error {
			fieldRows = append(fieldRows, batch...)
			return nil
		})

	if err != nil {
		qlog.Error("Failed to get entity schema: %v", err)
	}

	// Process the fields in sorted order
	for _, fr := range fieldRows {
		readPermissions := make([]qdata.EntityId, 0, len(fr.ReadPermissions))
		for _, id := range fr.ReadPermissions {
			readPermissions = append(readPermissions, qdata.EntityId(id))
		}

		writePermissions := make([]qdata.EntityId, 0, len(fr.WritePermissions))
		for _, id := range fr.WritePermissions {
			writePermissions = append(writePermissions, qdata.EntityId(id))
		}

		fieldSchema := new(qdata.FieldSchema).Init(
			entityType,
			qdata.FieldType(fr.FieldType),
			qdata.ValueType(fr.ValueType),
			qdata.FSORank(fr.Rank),
			qdata.FSOReadPermissions(readPermissions),
			qdata.FSOWritePermissions(writePermissions))

		// If it's a choice field, get the options
		if fieldSchema.ValueType == qdata.VTChoice {
			var options []string
			me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				err := tx.QueryRow(ctx, `
					SELECT options
					FROM ChoiceOptions
					WHERE entity_type = $1 AND field_type = $2
					`, entityType.AsString(), fr.FieldType).Scan(&options)
				if err != nil {
					if !errors.Is(err, pgx.ErrNoRows) {
						qlog.Error("Failed to get choice options: %v", err)
					}
				} else {
					fieldSchema.Choices = options
				}
			})
		}

		schema.Fields[fieldSchema.FieldType] = fieldSchema
	}

	return schema
}

func (me *PostgresStoreInteractor) PublishNotifications() qss.Signal[qdata.PublishNotificationArgs] {
	return me.publisherSig
}

func (me *PostgresStoreInteractor) PrepareQuery(sql string, args ...interface{}) *qdata.PageResult[*qdata.Entity] {
	qlog.Trace("PrepareQuery called with SQL: %s, args: %v", sql, args)
	pageOpts := []qdata.PageOpts{}
	typeHintOpts := []qdata.TypeHintOpts{}
	otherArgs := []interface{}{}

	for _, arg := range args {
		switch arg := arg.(type) {
		case qdata.PageOpts:
			pageOpts = append(pageOpts, arg)
		case qdata.TypeHintOpts:
			typeHintOpts = append(typeHintOpts, arg)
		default:
			otherArgs = append(otherArgs, arg)
		}
	}

	// Apply page options or use defaults
	pageConfig := qdata.DefaultPageConfig().ApplyOpts(pageOpts...)

	// Ensure we have a reasonable page size
	if pageConfig.PageSize <= 0 {
		pageConfig.PageSize = 100
	}

	// Parse the query
	fmtQuery := fmt.Sprintf(sql, otherArgs...)
	qlog.Trace("Formatted query: %s", fmtQuery)
	parsedQuery, err := qdata.ParseQuery(fmtQuery)
	if err != nil {
		qlog.Error("Failed to parse query: %v", err)
		return &qdata.PageResult[*qdata.Entity]{
			Items:    []*qdata.Entity{},
			CursorId: -1,
			NextPage: nil,
		}
	}
	qlog.Trace("Successfully parsed query. EntityType: %s, Fields: %+v", parsedQuery.Table.EntityType, parsedQuery.Fields)

	// Create SQLite builder
	builder, err := qdata.NewSQLiteBuilder(me)
	if err != nil {
		qlog.Error("Failed to create SQLite builder: %v", err)
		return &qdata.PageResult[*qdata.Entity]{
			Items:    []*qdata.Entity{},
			CursorId: -1,
			NextPage: nil,
		}
	}

	entityType := qdata.EntityType(parsedQuery.Table.EntityType)

	return &qdata.PageResult[*qdata.Entity]{
		Items:    []*qdata.Entity{},
		CursorId: pageConfig.CursorId,
		NextPage: func(ctx context.Context) (*qdata.PageResult[*qdata.Entity], error) {
			return builder.QueryWithPagination(ctx, entityType, parsedQuery, pageConfig.PageSize, pageConfig.CursorId, typeHintOpts...)
		},
		Cleanup: builder.Close,
	}
}
