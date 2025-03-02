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

		if parentId != "" {
			req := request.New().SetEntityId(parentId).SetFieldName("Children")
			me.fieldOperator.Read(ctx, req)
			if req.IsSuccessful() {
				children := req.GetValue().GetEntityList().GetEntities()
				children = append(children, entityId)
				req.GetValue().GetEntityList().SetEntities(children)
				me.fieldOperator.Write(ctx, req)
			}
		}
	})
	return entityId
}

func (me *EntityManager) FindEntities(ctx context.Context, entityType string) []string {
	processRows := func(rows pgx.Rows) []string {
		var entities []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				log.Error("Failed to scan entity ID: %v", err)
				continue
			}
			entities = append(entities, id)
		}
		return entities
	}

	entities := []string{}
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
		SELECT id FROM Entities WHERE type = $1
	`, entityType)
		if err != nil {
			log.Error("Failed to find entities: %v", err)
			return
		}
		defer rows.Close()

		entities = processRows(rows)
	})

	return entities
}

func (s *EntityManager) GetEntityTypes(ctx context.Context) []string {
	processRows := func(rows pgx.Rows) []string {
		var types []string
		for rows.Next() {
			var entityType string
			if err := rows.Scan(&entityType); err != nil {
				log.Error("Failed to scan entity type: %v", err)
				continue
			}
			types = append(types, entityType)
		}
		return types
	}

	types := []string{}
	s.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
		SELECT DISTINCT entity_type FROM EntitySchema
	`)
		if err != nil {
			log.Error("Failed to get entity types: %v", err)
			return
		}
		defer rows.Close()
		types = processRows(rows)
	})

	return types
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
		exists := false
		err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM Entities WHERE id = $1)`, entityId).Scan(&exists)
		if err != nil || !exists {
			return
		}

		// Remove references to this entity from other entities
		rows, err := tx.Query(ctx, `
            SELECT referenced_by_entity_id, referenced_by_field_name 
            FROM ReverseEntityReferences 
            WHERE referenced_entity_id = $1
        `, entityId)
		if err != nil {
			log.Error("Failed to query reverse references: %v", err)
		} else {
			defer rows.Close()

			// Process each reference to this entity
			for rows.Next() {
				var refByEntityId, refByFieldName string
				if err := rows.Scan(&refByEntityId, &refByFieldName); err != nil {
					log.Error("Failed to scan reverse reference data: %v", err)
					continue
				}

				// Read the current value
				req := request.New().SetEntityId(refByEntityId).SetFieldName(refByFieldName)
				me.fieldOperator.Read(ctx, req)

				if !req.IsSuccessful() {
					log.Error("Failed to read field %s for entity %s", refByFieldName, refByEntityId)
					continue
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

					req.GetValue().GetEntityList().SetEntities(updatedEntities)
					me.fieldOperator.Write(ctx, req)
				}
			}

			// Clean up the reverse references table
			_, err = tx.Exec(ctx, `
                DELETE FROM ReverseEntityReferences WHERE referenced_entity_id = $1
                OR referenced_by_entity_id = $1
            `, entityId)
			if err != nil {
				log.Error("Failed to delete reverse references: %v", err)
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
