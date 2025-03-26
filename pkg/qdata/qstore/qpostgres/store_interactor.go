package qpostgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qdata/qsnapshot"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

type PostgresStoreInteractor struct {
	core PostgresCore
}

func (me *PostgresStoreInteractor) GetEntity(ctx context.Context, entityId qdata.EntityId) *qdata.Entity {
	var result *qdata.Entity

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// First get the entity's basic info
		row := tx.QueryRow(ctx, `
		SELECT id, type
		FROM Entities
		WHERE id = $1
		`, entityId)

		var entityType string
		err := row.Scan(&entityId, &entityType)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				qlog.Error("Failed to get entity: %v", err)
			}
			return
		}

		result = &qdata.Entity{
			EntityId:   entityId,
			EntityType: qdata.EntityType(entityType),
		}
	})

	return result
}

func (me *PostgresStoreInteractor) CreateEntity(ctx context.Context, entityType qdata.EntityType, parentId qdata.EntityId, name string) qdata.EntityId {
	entityId := qdata.EntityId(uuid.New().String())

	for me.EntityExists(ctx, entityId) {
		entityId = qdata.EntityId(uuid.New().String())
	}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `
            INSERT INTO Entities (id, type)
            VALUES ($1, $2)
        `, entityId, entityType)

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
	return entityId
}

func (me *PostgresStoreInteractor) FindEntities(ctx context.Context, entityType qdata.EntityType) []qdata.EntityId {
	entities := []qdata.EntityId{}

	err := BatchedQuery(me.core, ctx,
		`SELECT id, cursor_id FROM Entities WHERE type = $1`,
		[]any{entityType},
		0, // use default batch size
		func(rows pgx.Rows, cursorId *int64) (string, error) {
			var id string
			err := rows.Scan(&id, cursorId)
			return id, err
		},
		func(batch []string) error {
			entities = append(entities, qdata.CastStringSliceToEntityIdSlice(batch)...)
			return nil
		},
	)

	if err != nil {
		qlog.Error("Failed to find entities: %v", err)
	}

	return entities
}

func (me *PostgresStoreInteractor) GetEntityTypes(ctx context.Context) []qdata.EntityType {
	entityTypes := []qdata.EntityType{}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
			SELECT DISTINCT entity_type
			FROM EntitySchema
		`)
		if err != nil {
			qlog.Error("Failed to get entity types: %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var entityType string
			if err := rows.Scan(&entityType); err != nil {
				qlog.Error("Failed to scan entity type: %v", err)
				continue
			}
			entityTypes = append(entityTypes, qdata.EntityType(entityType))
		}
	})

	return entityTypes
}

func (me *PostgresStoreInteractor) DeleteEntity(ctx context.Context, entityId qdata.EntityId) {
	// Collect all entities to delete in the correct order (children before parents)
	entitiesToDelete := me.collectDeletionOrderIterative(ctx, entityId)

	// Delete entities in the correct order (children first)
	for _, id := range entitiesToDelete {
		me.deleteEntityWithoutChildren(ctx, id)
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
		for _, table := range qfield.Types() {
			tableName := table + "s" // abbreviated
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

			tableName := getTableForType(schema.GetFieldType())
			if tableName == "" {
				qlog.Error("Invalid field type %s for field %s->%s", schema.GetFieldType(), entity.EntityType, indirectField)
				continue
			}

			if authorizer, ok := ctx.Value(qdata.FieldAuthorizerKey).(qdata.FieldAuthorizer); ok {
				if authorizer != nil && !authorizer.IsAuthorized(ctx, indirectEntity, indirectField, false) {
					qlog.Error("%s is not authorized to read from field: %s->%s", authorizer.AccessorId(), req.EntityId, req.FieldType)
					continue
				}
			}

			row := tx.QueryRow(ctx, fmt.Sprintf(`
					SELECT field_value, write_time, writer
					FROM %s
					WHERE entity_id = $1 AND field_type = $2
				`, tableName), indirectEntity, indirectField)

			var fieldValue interface{}
			var writeTime time.Time
			var writer string

			err := row.Scan(&fieldValue, &writeTime, &writer)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					qlog.Error("Failed to read field: %v", err)
				}

				continue
			}

			value := convertToValue(schema.GetFieldType(), fieldValue)
			if value == nil {
				qlog.Error("Failed to convert value for field %s->%s", entity.EntityType, indirectField)
				continue
			}

			req.SetValue(value)
			req.SetWriteTime(&writeTime)
			req.SetWriter(&writer)
			req.SetSuccessful(true)
		}
	})
}

func (me *PostgresStoreInteractor) Write(ctx context.Context, requests ...*qdata.Request) {
	ir := qquery.NewIndirectionResolver(me.me)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, req := range requests {
			indirectEntity, indirectField := ir.Resolve(ctx, req.EntityId, req.FieldType)
			if indirectField == "" || indirectEntity == "" {
				qlog.Error("Failed to resolve indirection for: %s->%s", req.EntityId, req.FieldType)
				continue
			}

			// Get entity and schema info in a single query
			var entityType string
			schema := &qprotobufs.DatabaseFieldSchema{}
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
			`, indirectEntity, indirectField).Scan(&entityType, &schema.Name, &schema.Type)

			if err != nil {
				qlog.Error("Failed to get entity and schema info: %v", err)
				continue
			}

			tableName := getTableForType(schema.Type)
			if tableName == "" {
				qlog.Error("Invalid field type %s for field %s->%s", schema.Type, entityType, indirectField)
				continue
			}

			if req.Value.IsNil() {
				anyPbField := fieldTypeToProtoType(schema.Type)
				req.SetValue(qfield.FromAnyPb(&anyPbField))
			}

			oldReq := new(qdata.Request).Init(req.EntityId, req.FieldType)
			me.Read(ctx, oldReq)

			if oldReq.Success && req.GetWriteOpt() == qdata.WriteChanges {
				if proto.Equal(qfield.ToAnyPb(oldReq.Value), qfield.ToAnyPb(req.Value)) {
					req.SetSuccessful(true)
					continue
				}
			}

			fieldValue := fieldValueToInterface(req.Value)
			if fieldValue == nil {
				qlog.Error("Failed to convert value for field %s->%s", entityType, indirectField)
				continue
			}

			if req.GetWriteTime() == nil {
				wt := time.Now()
				req.SetWriteTime(&wt)
			}

			if req.GetWriter() == nil {
				wr := ""

				if me.clientId == nil && qapp.GetName() != "" {
					clients := qquery.New(&qdata.LimitedStore{
						PostgresStoreInteractor: me,
						me.
							NotificationPublisher: me.notificationPublisher,
						PostgresStoreInteractor: me,
					}).Select().
						From("Client").
						Where(qdata.FTName).Equals(qapp.GetName()).
						Execute(ctx)

					if len(clients) == 0 {
						qlog.Error("Failed to get client id")
					} else {
						if len(clients) > 1 {
							qlog.Warn("Multiple clients found: %v", clients)
						}

						clientId := clients[0].GetId()
						me.clientId = &clientId
					}
				}

				if me.clientId != nil {
					wr = *me.clientId
				}

				req.SetWriter(&wr)
			}

			if authorizer, ok := ctx.Value(qdata.FieldAuthorizerKey).(qdata.FieldAuthorizer); ok {
				if authorizer != nil && !authorizer.IsAuthorized(ctx, indirectEntity, indirectField, true) {
					qlog.Error("%s is not authorized to write to field: %s->%s", authorizer.AccessorId(), req.EntityId, req.FieldType)
					continue
				}
			}

			if oldReq.Success && (oldReq.Value.IsEntityReference() || oldReq.Value.IsEntityList()) {
				var oldReferences []string
				if oldReq.Value.IsEntityReference() {
					oldRef := oldReq.Value.GetEntityReference()
					if oldRef != "" {
						oldReferences = []string{oldRef}
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
                        `, oldRef, indirectEntity, indirectField)

					if err != nil {
						qlog.Error("Failed to delete old reverse entity reference: %v", err)
					}
				}
			}

			if req.Value.IsEntityReference() || req.Value.IsEntityList() {
				var newReferences []string
				if req.Value.IsEntityReference() {
					newRef := req.Value.GetEntityReference()
					if newRef != "" && me.EntityExists(ctx, newRef) {
						newReferences = []string{newRef}
						req.Value.SetEntityReference(newRef)
					} else {
						req.Value.SetEntityReference("")
					}
				} else if req.Value.IsEntityList() {
					for _, newRef := range req.Value.GetEntityList() {
						if newRef != "" && me.EntityExists(ctx, newRef) {
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
                    `, newRef, indirectEntity, indirectField)

					if err != nil {
						qlog.Error("Failed to insert reverse entity reference: %v", err)
					}
				}
			}

			// Upsert the field value
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO %s (entity_id, field_type, field_value, write_time, writer)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (entity_id, field_type) 
				DO UPDATE SET field_value = $3, write_time = $4, writer = $5
			`, tableName), indirectEntity, indirectField, fieldValue, *req.GetWriteTime(), *req.GetWriter())

			if err != nil {
				qlog.Error("Failed to write field: %v", err)
				continue
			}

			// Handle notifications
			if me.notificationPublisher != nil {
				me.notificationPublisher.PublishNotifications(ctx, req, oldReq)
			}
			req.SetSuccessful(true)
		}
	})
}

func (me *PostgresStoreInteractor) InitializeIfRequired(ctx context.Context) {
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

func (me *PostgresStoreInteractor) RestoreSnapshot(ctx context.Context, ss qdata.Snapshot) {
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

		for _, schema := range ss.GetSchemas() {
			me.SetEntitySchema(ctx, schema)
		}

		// Restore entities
		for _, e := range ss.GetEntities() {
			_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, type)
			VALUES ($1, $2)
		`, e.GetId(), e.EntityType)
			if err != nil {
				qlog.Error("Failed to restore entity: %v", err)
				continue
			}
		}

		// Restore fields
		for _, f := range ss.Fields {
			req := qrequest.FromField(f)
			me.fieldOperator.Write(ctx, req)
		}

		// Restore schemas again because permissions are missed in the first pass
		for _, schema := range ss.GetSchemas() {
			me.SetEntitySchema(ctx, schema)
		}
	})
}

func (me *PostgresStoreInteractor) CreateSnapshot(ctx context.Context) qdata.Snapshot {
	ss := qsnapshot.New()

	// Get all entity types and their schemas
	entityTypes := me.GetEntityTypes(ctx)

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, entityType := range entityTypes {
			// Add schema
			schema := me.GetEntitySchema(ctx, entityType)
			if schema != nil {
				ss.AppendSchema(schema)

				// Add entities of this type and their fields
				entities := me.FindEntities(ctx, entityType)
				for _, entityId := range entities {
					entity := me.GetEntity(ctx, entityId)
					if entity != nil {
						ss.AppendEntity(entity)

						// Add fields for this entity
						for _, fieldName := range schema.GetFieldNames() {
							req := new(qdata.Request).Init(entityId, fieldName)
							me.fieldOperator.Read(ctx, req)
							if req.Success {
								ss.AppendField(qfield.FromRequest(req))
							}
						}
					}
				}
			}
		}
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

func (me *PostgresStoreInteractor) GetFieldSchema(ctx context.Context, entityType qdata.EntityType, fieldType qdata.EntityType) qdata.FieldSchema {
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

	schema := qfield.FromSchemaPb(schemaPb)
	if schema.IsChoice() {
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

		schema.AsChoiceFieldSchema().SetChoices(options)
	}

	return schema
}

func (me *PostgresStoreInteractor) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema qdata.FieldSchema) {
	// Find entity schema
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		qlog.Error("Failed to get entity schema for type %s", entityType)
		return
	}

	// Updating existing field schema under entity schema or append new field schema
	updated := false
	fields := entitySchema.Fields
	for i, field := range fields {
		if field.FieldType == fieldName {
			fields[i] = schema
			updated = true
			break
		}
	}

	if !updated {
		fields = append(fields, schema)
	}

	entitySchema.SetFields(fields)

	me.SetEntitySchema(ctx, entitySchema)
}

func (me *PostgresStoreInteractor) FieldExists(ctx context.Context, fieldName, entityType qdata.EntityType) bool {
	exists := false

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM EntitySchema 
				WHERE entity_type = $1 AND field_type = $2
			)
		`, entityType, fieldName).Scan(&exists)
		if err != nil {
			qlog.Error("Failed to check field existence: %v", err)
		}
	})

	return exists
}

func (me *PostgresStoreInteractor) SetEntitySchema(ctx context.Context, requestedSchema qdata.EntitySchema) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get existing schema for comparison
		oldSchema := me.GetEntitySchema(ctx, requestedSchema.EntityType)

		// Delete existing schema
		_, err := tx.Exec(ctx, `
			DELETE FROM EntitySchema WHERE entity_type = $1
		`, requestedSchema.EntityType)
		if err != nil {
			qlog.Error("Failed to delete existing schema: %v", err)
			return
		}

		// Delete existing choice options
		_, err = tx.Exec(ctx, `
			DELETE FROM ChoiceOptions WHERE entity_type = $1
		`, requestedSchema.EntityType)
		if err != nil {
			qlog.Error("Failed to delete existing choice options: %v", err)
			return
		}

		// Build new schema
		fields := []qdata.FieldSchema{}
		if !slices.ContainsFunc(requestedSchema.Fields, func(field qdata.FieldSchema) bool {
			return field.FieldType == qdata.FTName
		}) {
			fields = append(fields, qfield.NewSchema(qdata.FTName, qfield.String))
		}

		if !slices.ContainsFunc(requestedSchema.Fields, func(field qdata.FieldSchema) bool {
			return field.FieldType == qdata.FTDescription
		}) {
			fields = append(fields, qfield.NewSchema(qdata.FTDescription, qfield.String))
		}

		if !slices.ContainsFunc(requestedSchema.Fields, func(field qdata.FieldSchema) bool {
			return field.FieldType == qdata.FTParent
		}) {
			fields = append(fields, qfield.NewSchema(qdata.FTParent, qfield.EntityReference))
		}

		if !slices.ContainsFunc(requestedSchema.Fields, func(field qdata.FieldSchema) bool {
			return field.FieldType == qdata.FTChildren
		}) {
			fields = append(fields, qfield.NewSchema(qdata.FTChildren, qfield.EntityList))
		}

		fields = append(fields, requestedSchema.Fields...)
		modifiableSchema := qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{})
		modifiableSchema.SetType(requestedSchema.EntityType)
		modifiableSchema.SetFields(fields)

		for i, field := range modifiableSchema.Fields {
			// Remove non-existant entity ids from read/write permissions
			readPermissions := []string{}
			for _, id := range field.GetReadPermissions() {
				entity := me.GetEntity(ctx, id)
				if entity != nil && entity.EntityType == qdata.ETPermission {
					readPermissions = append(readPermissions, id)
				}
			}

			writePermissions := []string{}
			for _, id := range field.GetWritePermissions() {
				entity := me.GetEntity(ctx, id)
				if entity != nil && entity.EntityType == qdata.ETPermission {
					writePermissions = append(writePermissions, id)
				}
			}

			_, err = tx.Exec(ctx, `
            INSERT INTO EntitySchema (entity_type, field_type, value_type, read_permissions, write_permissions, rank)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (entity_type, field_type) 
            DO UPDATE SET value_type = $3, read_permissions = $4, write_permissions = $5, rank = $6
        `, modifiableSchema.EntityType, field.FieldType, field.GetFieldType(), readPermissions, writePermissions, i)

			if err != nil {
				qlog.Error("Failed to set field schema: %v", err)
				return
			}

			// Handle choice options if this is a choice field
			if field.IsChoice() {
				choiceSchema := field.AsChoiceFieldSchema()
				options := choiceSchema.GetChoices()

				_, err = tx.Exec(ctx, `
                INSERT INTO ChoiceOptions (entity_type, field_type, options)
                VALUES ($1, $2, $3)
                ON CONFLICT (entity_type, field_type)
                DO UPDATE SET options = $3
            `, modifiableSchema.EntityType, field.FieldType, options)

				if err != nil {
					qlog.Error("Failed to set choice options: %v", err)
					return
				}
			}
		}

		// Handle field changes for existing entities
		if oldSchema != nil {
			removedFields := []string{}
			newFields := []string{}

			// Find removed fields
			for _, oldField := range oldSchema.Fields {
				found := false
				for _, newField := range modifiableSchema.Fields {
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
			for _, newField := range modifiableSchema.Fields {
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

			// Update existing entities
			entities := me.FindEntities(ctx, modifiableSchema.EntityType)
			for _, entityId := range entities {
				// Remove deleted fields
				for _, fieldName := range removedFields {
					tableName := getTableForType(oldSchema.GetField(fieldName).GetFieldType())
					if tableName == "" {
						continue
					}
					_, err = tx.Exec(ctx, fmt.Sprintf(`
						DELETE FROM %s 
						WHERE entity_id = $1 AND field_type = $2
					`, tableName), entityId, fieldName)
					if err != nil {
						qlog.Error("Failed to delete field: %v", err)
						continue
					}
				}

				// Initialize new fields
				for _, fieldName := range newFields {
					req := new(qdata.Request).Init(entityId, fieldName)
					me.fieldOperator.Write(ctx, req)
				}
			}
		}
	})
}

func (me *PostgresStoreInteractor) GetEntitySchema(ctx context.Context, entityType qdata.EntityType) *qdata.EntitySchema {
	schema := &qdata.EntitySchema{}

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
		[]any{string(entityType)},
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

		fieldSchema := &qdata.FieldSchema{
			EntityType:       entityType,
			FieldType:        qdata.FieldType(fr.FieldType),
			ValueType:        qdata.ValueType(fr.ValueType),
			Rank:             fr.Rank,
			ReadPermissions:  readPermissions,
			WritePermissions: writePermissions,
		}

		// If it's a choice field, get the options
		if fieldSchema.ValueType == qdata.VTChoice {
			var options []string
			me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				err := tx.QueryRow(ctx, `
					SELECT options
					FROM ChoiceOptions
					WHERE entity_type = $1 AND field_type = $2
				`, string(entityType), fr.FieldType).Scan(&options)
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
