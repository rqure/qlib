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

func NewSnapshotManager(core Core) data.ModifiableSnapshotManager {
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
				DROP INDEX IF EXISTS idx_reverseentityreferences_referenced_by_field_name;
				
				-- Drop write time indexes
				DROP INDEX IF EXISTS idx_strings_write_time;
				DROP INDEX IF EXISTS idx_entityreferences_write_time;
				DROP INDEX IF EXISTS idx_timestamps_write_time;
				DROP INDEX IF EXISTS idx_timestamps_field_value;
				
				-- Drop choice option indexes
				DROP INDEX IF EXISTS idx_choiceoptions_entity_type;
			`)

		if err != nil {
			log.Error("Failed to drop indexes: %v", err)
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
			log.Error("Failed to clear existing data: %v", err)
			return
		}

		// Recreate tables and indexes
		if err := me.initializeDatabase(ctx); err != nil {
			log.Error("Failed to initialize database: %v", err)
			return
		}

		for _, schema := range ss.GetSchemas() {
			me.schemaManager.SetEntitySchema(ctx, schema)
		}

		// Restore entities
		for _, e := range ss.GetEntities() {
			_, err := tx.Exec(ctx, `
			INSERT INTO Entities (id, type)
			VALUES ($1, $2)
		`, e.GetId(), e.GetType())
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

		// Restore schemas again because permissions are missed in the first pass
		for _, schema := range ss.GetSchemas() {
			me.schemaManager.SetEntitySchema(ctx, schema)
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
