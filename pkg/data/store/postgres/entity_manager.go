package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type EntityManager struct {
	core          Core
	schemaManager data.SchemaManager
	fieldOperator data.FieldOperator
}

func NewEntityManager(core Core) data.ModifiableEntityManager {
	return &EntityManager{
		core: core,
	}
}

func (me *EntityManager) SetSchemaManager(schemaManager data.SchemaManager) {
	me.schemaManager = schemaManager
}

func (me *EntityManager) SetFieldOperator(fieldOperator data.FieldOperator) {
	me.fieldOperator = fieldOperator
}

func (me *EntityManager) GetEntity(ctx context.Context, entityId string) data.Entity {
	var result data.Entity

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
				log.Error("Failed to get entity: %v", err)
			}
			return
		}

		entityPb := &protobufs.DatabaseEntity{
			Id:   entityId,
			Type: entityType,
		}

		result = entity.FromEntityPb(entityPb)
	})

	return result
}

func (me *EntityManager) CreateEntity(ctx context.Context, entityType, parentId, name string) string {
	entityId := uuid.New().String()

	for me.EntityExists(ctx, entityId) {
		entityId = uuid.New().String()
	}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		_, err := tx.Exec(ctx, `
            INSERT INTO Entities (id, type)
            VALUES ($1, $2)
        `, entityId, entityType)

		if err != nil {
			log.Error("Failed to create entity: %v", err)
			entityId = ""
			return
		}

		// Initialize fields with default values
		schema := me.schemaManager.GetEntitySchema(ctx, entityType)
		if schema != nil {
			reqs := []data.Request{}
			for _, f := range schema.GetFields() {
				req := request.New().SetEntityId(entityId).SetFieldName(f.GetFieldName())

				if f.GetFieldName() == "Name" {
					req.GetValue().SetString(name)
				} else if f.GetFieldName() == "Parent" {
					req.GetValue().SetEntityReference(parentId)
				}
				reqs = append(reqs, req)
			}
			me.fieldOperator.Write(ctx, reqs...)
		}

		// Only update parent's children if parentId is provided
		if parentId != "" {
			req := request.New().SetEntityId(parentId).SetFieldName("Children")
			me.fieldOperator.Read(ctx, req)
			if req.IsSuccessful() {
				children := req.GetValue().GetEntityList().GetEntities()
				children = append(children, entityId)
				req.GetValue().SetEntityList(children)
				me.fieldOperator.Write(ctx, req)
			}
		}
	})
	return entityId
}

func (me *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	entities := []string{}

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
			entities = append(entities, batch...)
			return nil
		},
	)

	if err != nil {
		log.Error("Failed to find entities: %v", err)
	}

	return entities
}

func (me *EntityManager) GetEntityTypes(ctx context.Context) []string {
	entityTypes := []string{}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
			SELECT DISTINCT entity_type
			FROM EntitySchema
		`)
		if err != nil {
			log.Error("Failed to get entity types: %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var entityType string
			if err := rows.Scan(&entityType); err != nil {
				log.Error("Failed to scan entity type: %v", err)
				continue
			}
			entityTypes = append(entityTypes, entityType)
		}
	})

	return entityTypes
}

func (me *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	// Collect all entities to delete in the correct order (children before parents)
	entitiesToDelete := me.collectDeletionOrderIterative(ctx, entityId)

	// Delete entities in the correct order (children first)
	for _, id := range entitiesToDelete {
		me.deleteEntityWithoutChildren(ctx, id)
	}
}

// collectDeletionOrderIterative builds a list of entities to delete in the right order (children before parents)
// using an iterative depth-first traversal approach
func (me *EntityManager) collectDeletionOrderIterative(ctx context.Context, rootEntityId string) []string {
	var result []string

	// We need two data structures:
	// 1. A stack for DFS traversal
	// 2. A visited map to track which entities we've already processed
	type stackItem struct {
		id        string
		processed bool // Whether we've already processed children
	}

	stack := []stackItem{{id: rootEntityId, processed: false}}
	visited := make(map[string]bool)

	for len(stack) > 0 {
		// Get the top item from stack
		current := stack[len(stack)-1]

		if current.processed {
			// If we've already processed children, add to result and pop from stack
			stack = stack[:len(stack)-1]
			if !visited[current.id] {
				result = append(result, current.id)
				visited[current.id] = true
			}
		} else {
			// Mark as processed and get children
			stack[len(stack)-1].processed = true

			childrenReq := request.New().SetEntityId(current.id).SetFieldName("Children")
			me.fieldOperator.Read(ctx, childrenReq)

			if childrenReq.IsSuccessful() {
				children := childrenReq.GetValue().GetEntityList().GetEntities()

				// Add children to stack in reverse order (so we process in original order)
				for i := len(children) - 1; i >= 0; i-- {
					childId := children[i]
					// Only add if not already visited
					if !visited[childId] {
						stack = append(stack, stackItem{id: childId, processed: false})
					}
				}
			}
		}
	}

	return result
}

// deleteEntityWithoutChildren deletes a single entity, handling its references but not its children
func (me *EntityManager) deleteEntityWithoutChildren(ctx context.Context, entityId string) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Check if entity exists
		entity := me.GetEntity(ctx, entityId)
		if entity == nil {
			log.Error("Entity %s does not exist", entityId)
			return
		}

		type Ref struct {
			ByEntityId  string
			ByFieldName string
		}

		// Remove references to this entity from other entities
		err := BatchedQuery(me.core, ctx, `
            SELECT referenced_by_entity_id, referenced_by_field_name, cursor_id 
            FROM ReverseEntityReferences 
            WHERE referenced_entity_id = $1
        `,
			[]any{entityId},
			0,
			func(rows pgx.Rows, cursorId *int64) (Ref, error) {
				var ref Ref
				err := rows.Scan(&ref.ByEntityId, &ref.ByFieldName, cursorId)
				return ref, err
			},
			func(batch []Ref) error {
				for _, ref := range batch {

					// Read the current value
					req := request.New().SetEntityId(ref.ByEntityId).SetFieldName(ref.ByFieldName)
					me.fieldOperator.Read(ctx, req)

					if !req.IsSuccessful() {
						return fmt.Errorf("failed to read field %s for entity %s", ref.ByFieldName, ref.ByEntityId)
					}

					// Update the reference based on its type
					if req.GetValue().IsEntityReference() {
						// If it's a direct reference, clear it
						if req.GetValue().GetEntityReference() == entityId {
							req.GetValue().SetEntityReference("")
							me.fieldOperator.Write(ctx, req)
						}
					} else if req.GetValue().IsEntityList() {
						// If it's a list of references, remove this entity from the list
						entities := req.GetValue().GetEntityList().GetEntities()
						updatedEntities := []string{}

						for _, id := range entities {
							if id != entityId {
								updatedEntities = append(updatedEntities, id)
							}
						}

						req.GetValue().SetEntityList(updatedEntities)
						me.fieldOperator.Write(ctx, req)
					}
				}
				return nil
			},
		)

		if err != nil {
			log.Error("Failed to query reverse references: %v", err)
		} else {
			// Clean up the reverse references table
			_, err = tx.Exec(ctx, `
                DELETE FROM ReverseEntityReferences WHERE referenced_entity_id = $1
                OR referenced_by_entity_id = $1
            `, entityId)
			if err != nil {
				log.Error("Failed to delete reverse references: %v", err)
			}
		}

		if entity.GetType() == "Permission" {
			// Remove permissions from schemas
			_, err := tx.Exec(ctx, `
				UPDATE EntitySchema 
				SET read_permissions = array_remove(read_permissions, $1),
					write_permissions = array_remove(write_permissions, $1)
				WHERE $1 = ANY(read_permissions) OR $1 = ANY(write_permissions)
			`, entityId)

			if err != nil {
				log.Error("Failed to remove permission from schemas: %v", err)
			}
		}

		// Delete all field values
		for _, table := range field.Types() {
			tableName := table + "s" // abbreviated
			_, err := tx.Exec(ctx, fmt.Sprintf(`
                DELETE FROM %s WHERE entity_id = $1
            `, tableName), entityId)
			if err != nil {
				log.Error("Failed to delete fields from %s: %v", tableName, err)
				return
			}
		}

		// Finally delete the entity itself
		_, err = tx.Exec(ctx, `
            DELETE FROM Entities WHERE id = $1
        `, entityId)
		if err != nil {
			log.Error("Failed to delete entity: %v", err)
			return
		}
	})
}

func (me *EntityManager) EntityExists(ctx context.Context, entityId string) bool {
	exists := false

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM Entities WHERE id = $1)
		`, entityId).Scan(&exists)

		if err != nil {
			log.Error("Failed to check entity existence: %v", err)
		}
	})

	return exists
}
