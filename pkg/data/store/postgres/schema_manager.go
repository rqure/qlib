package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"

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

func NewSchemaManager(core Core) data.ModifiableSchemaManager {
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
	schemaPb := &protobufs.DatabaseFieldSchema{}
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
            SELECT field_name, field_type, read_permissions, write_permissions
            FROM EntitySchema
            WHERE entity_type = $1 AND field_name = $2
        `, entityType, fieldName).Scan(&schemaPb.Name, &schemaPb.Type, &schemaPb.ReadPermissions, &schemaPb.WritePermissions)
		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to get field schema: %v", err)
			}
			schemaPb = nil
		}
	})

	if schemaPb == nil {
		return nil
	}

	schema := field.FromSchemaPb(schemaPb)
	if schema.IsChoice() {
		var options []string
		me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
			err := tx.QueryRow(ctx, `
				SELECT options
				FROM ChoiceOptions
				WHERE entity_type = $1 AND field_name = $2
			`, entityType, fieldName).Scan(&options)
			if err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					log.Error("Failed to get choice options: %v", err)
				}
			}
		})

		schema.AsChoiceFieldSchema().SetChoices(options)
	}

	return schema
}

func (me *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	// Find entity schema
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		log.Error("Failed to get entity schema for type %s", entityType)
		return
	}

	// Updating existing field schema under entity schema or append new field schema
	updated := false
	fields := entitySchema.GetFields()
	for i, field := range fields {
		if field.GetFieldName() == fieldName {
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

		// Delete existing choice options
		_, err = tx.Exec(ctx, `
			DELETE FROM ChoiceOptions WHERE entity_type = $1
		`, schema.GetType())
		if err != nil {
			log.Error("Failed to delete existing choice options: %v", err)
			return
		}

		// Build new schema
		fields := []data.FieldSchema{}
		if !slices.ContainsFunc(schema.GetFields(), func(field data.FieldSchema) bool {
			return field.GetFieldName() == "Name"
		}) {
			schema.SetFields(append(fields, field.NewSchema("Name", field.String)))
		}

		if !slices.ContainsFunc(schema.GetFields(), func(field data.FieldSchema) bool {
			return field.GetFieldName() == "Description"
		}) {
			schema.SetFields(append(fields, field.NewSchema("Description", field.String)))
		}

		if !slices.ContainsFunc(schema.GetFields(), func(field data.FieldSchema) bool {
			return field.GetFieldName() == "Parent"
		}) {
			schema.SetFields(append(fields, field.NewSchema("Parent", field.EntityReference)))
		}

		if !slices.ContainsFunc(schema.GetFields(), func(field data.FieldSchema) bool {
			return field.GetFieldName() == "Children"
		}) {
			schema.SetFields(append(fields, field.NewSchema("Children", field.EntityList)))
		}

		fields = append(fields, schema.GetFields()...)
		schema.SetFields(fields)

		for i, field := range schema.GetFields() {
			// Remove non-existant entity ids from read/write permissions
			readPermissions := []string{}
			for _, id := range field.GetReadPermissions() {
				entity := me.entityManager.GetEntity(ctx, id)
				if entity != nil && entity.GetType() == data.EntityTypePermission {
					readPermissions = append(readPermissions, id)
				}
			}

			writePermissions := []string{}
			for _, id := range field.GetWritePermissions() {
				entity := me.entityManager.GetEntity(ctx, id)
				if entity != nil && entity.GetType() == data.EntityTypePermission {
					writePermissions = append(writePermissions, id)
				}
			}

			_, err = tx.Exec(ctx, `
            INSERT INTO EntitySchema (entity_type, field_name, field_type, read_permissions, write_permissions, rank)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (entity_type, field_name) 
            DO UPDATE SET field_type = $3, read_permissions = $4, write_permissions = $5, rank = $6
        `, schema.GetType(), field.GetFieldName(), field.GetFieldType(), readPermissions, writePermissions, i)

			if err != nil {
				log.Error("Failed to set field schema: %v", err)
				return
			}

			// Handle choice options if this is a choice field
			if field.IsChoice() {
				choiceSchema := field.AsChoiceFieldSchema()
				options := choiceSchema.GetChoices()

				_, err = tx.Exec(ctx, `
                INSERT INTO ChoiceOptions (entity_type, field_name, options)
                VALUES ($1, $2, $3)
                ON CONFLICT (entity_type, field_name)
                DO UPDATE SET options = $3
            `, schema.GetType(), field.GetFieldName(), options)

				if err != nil {
					log.Error("Failed to set choice options: %v", err)
					return
				}
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
