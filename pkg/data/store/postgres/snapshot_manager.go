package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/data/snapshot"
	"github.com/rqure/qlib/pkg/log"
)

type SnapshotManager struct {
	core          Core
	schemaManager data.SchemaManager
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewSnapshotManager(core Core) data.SnapshotManager {
	return &SnapshotManager{
		core: core,
	}
}

func (me *SnapshotManager) SetSchemaManager(schemaManager data.SchemaManager) {
	me.schemaManager = schemaManager
}

func (me *SnapshotManager) SetEntityManager(entityManager data.EntityManager) {
	me.entityManager = entityManager
}

func (me *SnapshotManager) SetFieldOperator(fieldOperator data.FieldOperator) {
	me.fieldOperator = fieldOperator
}

func (me *SnapshotManager) RestoreSnapshot(ctx context.Context, ss data.Snapshot) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Remove existing tables
		_, err := tx.Exec(ctx, `
			DROP TABLE IF EXISTS
				Entities, EntitySchema, Strings,
				BinaryFiles, Ints, Floats, Bools, EntityReferences,
				Timestamps, Transformations, NotificationConfigEntityId,
				NotificationConfigEntityType, Notifications
			CASCADE
		`)

		if err != nil {
			log.Error("Failed to clear existing data: %v", err)
			return
		}

		// Recreate tables and indexes
		if err := me.initializeDatabase(ctx); err != nil {
			log.Error("Failed to initialize database: %v", err)
			return
		}

		for _, schema := range ss.GetSchemas() {
			for i, field := range schema.GetFields() {
				_, err := tx.Exec(ctx, `
				INSERT INTO EntitySchema (entity_type, field_name, field_type, rank)
				VALUES ($1, $2, $3, $4)
			`, schema.GetType(), field.GetFieldName(), field.GetFieldType(), i)
				if err != nil {
					log.Error("Failed to restore schema: %v", err)
					continue
				}
			}
		}

		// Restore entities
		for _, e := range ss.GetEntities() {
			_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, name, parent_id, type)
			VALUES ($1, $2, $3, $4)
		`, e.GetId(), e.GetName(), e.GetParentId(), e.GetType())
			if err != nil {
				log.Error("Failed to restore entity: %v", err)
				continue
			}
		}

		// Restore fields
		for _, f := range ss.GetFields() {
			req := request.FromField(f)
			me.fieldOperator.Write(ctx, req)
		}
	})
}

func (me *SnapshotManager) CreateSnapshot(ctx context.Context) data.Snapshot {
	ss := snapshot.New()

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get all entity types and their schemas
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

			// Add schema
			schema := me.schemaManager.GetEntitySchema(ctx, entityType)
			if schema != nil {
				ss.AppendSchema(schema)

				// Add entities of this type and their fields
				entities := me.entityManager.FindEntities(ctx, entityType)
				for _, entityId := range entities {
					entity := me.entityManager.GetEntity(ctx, entityId)
					if entity != nil {
						ss.AppendEntity(entity)

						// Add fields for this entity
						for _, fieldName := range schema.GetFieldNames() {
							req := request.New().SetEntityId(entityId).SetFieldName(fieldName)
							me.fieldOperator.Read(ctx, req)
							if req.IsSuccessful() {
								ss.AppendField(field.FromRequest(req))
							}
						}
					}
				}
			}
		}

	})

	return ss
}

func (me *SnapshotManager) initializeDatabase(ctx context.Context) error {
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
