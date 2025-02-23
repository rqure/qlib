package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
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
	var e data.Entity

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// First get the entity's basic info
		row := tx.QueryRow(ctx, `
		SELECT id, name, parent_id, type
		FROM Entities
		WHERE id = $1
		`, entityId)

		var name, parentId, entityType string
		err := row.Scan(&entityId, &name, &parentId, &entityType)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to get entity: %v", err)
			}
			return
		}

		// Then get children using parent_id relationship
		rows, err := tx.Query(ctx, `
		SELECT id
		FROM Entities
		WHERE parent_id = $1
		`, entityId)
		if err != nil {
			log.Error("Failed to get children: %v", err)
			return
		}
		defer rows.Close()

		var children []string
		for rows.Next() {
			var childId string
			if err := rows.Scan(&childId); err != nil {
				log.Error("Failed to scan child id: %v", err)
				continue
			}
			children = append(children, childId)
		}

		de := &protobufs.DatabaseEntity{
			Id:   entityId,
			Name: name,
			Parent: &protobufs.EntityReference{
				Raw: parentId,
			},
			Type: entityType,
		}

		de.Children = make([]*protobufs.EntityReference, len(children))
		for i, child := range children {
			de.Children[i] = &protobufs.EntityReference{Raw: child}
		}

		e = entity.FromEntityPb(de)
	})

	return e
}

func (me *EntityManager) SetEntity(ctx context.Context, e data.Entity) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Update entity or create if it doesn't exist
		_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, name, parent_id, type)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE
			SET name = $2, parent_id = $3, type = $4
		`, e.GetId(), e.GetName(), e.GetParentId(), e.GetType())

		if err != nil {
			log.Error("Failed to update entity: %v", err)
			return
		}
	})
}

func (me *EntityManager) CreateEntity(ctx context.Context, entityType, parentId, name string) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		entityId := uuid.New().String()
		_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, name, parent_id, type)
			VALUES ($1, $2, $3, $4)
		`, entityId, name, parentId, entityType)

		if err != nil {
			log.Error("Failed to create entity: %v", err)
			return
		}

		// Initialize fields with default values
		schema := me.schemaManager.GetEntitySchema(ctx, entityType)
		if schema != nil {
			for _, f := range schema.GetFields() {
				req := request.New().SetEntityId(entityId).SetFieldName(f.GetFieldName())
				me.fieldOperator.Write(ctx, req)
			}
		}
	})
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

// Fix DeleteEntity to clean up notifications
func (me *EntityManager) DeleteEntity(ctx context.Context, entityId string) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get children using parent_id relationship
		rows, err := tx.Query(ctx, `
			SELECT id FROM Entities WHERE parent_id = $1
		`, entityId)
		if err != nil {
			log.Error("Failed to get children: %v", err)
			return
		}
		defer rows.Close()

		// Recursively delete children
		for rows.Next() {
			var childId string
			if err := rows.Scan(&childId); err != nil {
				log.Error("Failed to scan child id: %v", err)
				continue
			}
			me.DeleteEntity(ctx, childId)
		}

		// Delete all field values
		for _, table := range []string{"Strings", "BinaryFiles", "Ints", "Floats", "Bools",
			"EntityReferences", "Timestamps", "Transformations"} {
			_, err := tx.Exec(ctx, fmt.Sprintf(`
				DELETE FROM %s WHERE entity_id = $1
			`, table), entityId)
			if err != nil {
				log.Error("Failed to delete fields from %s: %v", table, err)
				return
			}
		}

		// Delete notification configs and their notifications
		_, err = tx.Exec(ctx, `
			WITH deleted_configs AS (
				DELETE FROM NotificationConfigEntityId 
				WHERE entity_id = $1
				RETURNING service_id, 
						encode(
							notification::bytea, 
							'base64'
						) as token
			)
			DELETE FROM Notifications 
			WHERE service_id IN (SELECT service_id FROM deleted_configs)
			AND notification::text = ANY(SELECT token FROM deleted_configs);
		`, entityId)
		if err != nil {
			log.Error("Failed to delete notification configs: %v", err)
			return
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
