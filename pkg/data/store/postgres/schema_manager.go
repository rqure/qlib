package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/entity"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
)

type SchemaManager struct {
	core          Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewSchemaManager(core Core) data.SchemaManager {
	return &SchemaManager{
		core: core,
	}
}

func (me *SchemaManager) SetEntityManager(entityManager data.EntityManager) {
	me.entityManager = entityManager
}

func (me *SchemaManager) SetFieldOperator(fieldOperator data.FieldOperator) {
	me.fieldOperator = fieldOperator
}

func (me *SchemaManager) GetFieldSchema(ctx context.Context, entityType, fieldName string) data.FieldSchema {
	schema := &protobufs.DatabaseFieldSchema{}
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT field_name, field_type
			FROM EntitySchema
			WHERE entity_type = $1 AND field_name = $2
		`, entityType, fieldName).Scan(&schema.Name, &schema.Type)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to get field schema: %v", err)
			}
			schema = nil
		}
	})

	if schema == nil {
		return nil
	}

	return field.FromSchemaPb(schema)
}

func (me *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get current schema to handle field type changes
		oldSchema := me.GetFieldSchema(ctx, entityType, fieldName)

		// Update or insert the field schema
		_, err := tx.Exec(ctx, `
			INSERT INTO EntitySchema (entity_type, field_name, field_type, rank)
			VALUES ($1, $2, $3, 0)
			ON CONFLICT (entity_type, field_name) 
			DO UPDATE SET field_type = $3
		`, entityType, fieldName, schema.GetFieldType())

		if err != nil {
			log.Error("Failed to set field schema: %v", err)
			return
		}

		// If field type changed, migrate existing data
		if oldSchema != nil && oldSchema.GetFieldType() != schema.GetFieldType() {
			oldTable := getTableForType(oldSchema.GetFieldType())
			newTable := getTableForType(schema.GetFieldType())

			if oldTable != "" && newTable != "" {
				// Delete old field values
				_, err = tx.Exec(ctx, fmt.Sprintf(`
					DELETE FROM %s 
					WHERE entity_id IN (
						SELECT id FROM Entities WHERE type = $1
					) AND field_name = $2
				`, oldTable), entityType, fieldName)

				if err != nil {
					log.Error("Failed to delete old field values: %v", err)
					return
				}
			}

			// Initialize new field values for all entities of this type
			entities := me.entityManager.FindEntities(ctx, entityType)
			for _, entityId := range entities {
				req := request.New().SetEntityId(entityId).SetFieldName(fieldName)
				me.fieldOperator.Write(ctx, req)
			}
		}
	})
}

func (me *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	exists := false

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM EntitySchema 
				WHERE entity_type = $1 AND field_name = $2
			)
		`, entityType, fieldName).Scan(&exists)
		if err != nil {
			log.Error("Failed to check field existence: %v", err)
		}
	})

	return exists
}

func (me *SchemaManager) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get existing schema for comparison
		oldSchema := me.GetEntitySchema(ctx, schema.GetType())

		// Delete existing schema
		_, err := tx.Exec(ctx, `
			DELETE FROM EntitySchema WHERE entity_type = $1
		`, schema.GetType())
		if err != nil {
			log.Error("Failed to delete existing schema: %v", err)
			return
		}

		// Insert new schema
		for i, field := range schema.GetFields() {
			_, err = tx.Exec(ctx, `
				INSERT INTO EntitySchema (entity_type, field_name, field_type, rank)
				VALUES ($1, $2, $3, $4)
			`, schema.GetType(), field.GetFieldName(), field.GetFieldType(), i)
			if err != nil {
				log.Error("Failed to insert field schema: %v", err)
				return
			}
		}

		// Handle field changes for existing entities
		if oldSchema != nil {
			removedFields := []string{}
			newFields := []string{}

			// Find removed fields
			for _, oldField := range oldSchema.GetFields() {
				found := false
				for _, newField := range schema.GetFields() {
					if oldField.GetFieldName() == newField.GetFieldName() {
						found = true
						break
					}
				}
				if !found {
					removedFields = append(removedFields, oldField.GetFieldName())
				}
			}

			// Find new fields
			for _, newField := range schema.GetFields() {
				found := false
				for _, oldField := range oldSchema.GetFields() {
					if newField.GetFieldName() == oldField.GetFieldName() {
						found = true
						break
					}
				}
				if !found {
					newFields = append(newFields, newField.GetFieldName())
				}
			}

			// Update existing entities
			entities := me.entityManager.FindEntities(ctx, schema.GetType())
			for _, entityId := range entities {
				// Remove deleted fields
				for _, fieldName := range removedFields {
					tableName := getTableForType(oldSchema.GetField(fieldName).GetFieldType())
					if tableName == "" {
						continue
					}
					_, err = tx.Exec(ctx, fmt.Sprintf(`
						DELETE FROM %s 
						WHERE entity_id = $1 AND field_name = $2
					`, tableName), entityId, fieldName)
					if err != nil {
						log.Error("Failed to delete field: %v", err)
						continue
					}
				}

				// Initialize new fields
				for _, fieldName := range newFields {
					req := request.New().SetEntityId(entityId).SetFieldName(fieldName)
					me.fieldOperator.Write(ctx, req)
				}
			}
		}
	})
}

func (me *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	processRows := func(rows pgx.Rows) data.EntitySchema {
		schema := entity.FromSchemaPb(&protobufs.DatabaseEntitySchema{})
		schema.SetType(entityType)
		var fields []data.FieldSchema

		for rows.Next() {
			var fieldName, fieldType string
			var rank int
			if err := rows.Scan(&fieldName, &fieldType, &rank); err != nil {
				log.Error("Failed to scan field schema: %v", err)
				continue
			}
			fields = append(fields, field.FromSchemaPb(&protobufs.DatabaseFieldSchema{
				Name: fieldName,
				Type: fieldType,
			}))
		}

		schema.SetFields(fields)
		return schema
	}

	var schema data.EntitySchema
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
			SELECT field_name, field_type, rank
			FROM EntitySchema
			WHERE entity_type = $1
			ORDER BY rank
		`, entityType)
		if err != nil {
			log.Error("Failed to get entity schema: %v", err)
			return
		}
		defer rows.Close()
		schema = processRows(rows)
	})

	return schema
}

func (me *SchemaManager) EntityExists(ctx context.Context, entityId string) bool {
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
