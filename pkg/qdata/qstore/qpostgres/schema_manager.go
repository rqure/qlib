package qpostgres

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qentity"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qdata/qrequest"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type SchemaManager struct {
	core          Core
	entityManager qdata.EntityManager
	fieldOperator qdata.FieldOperator
}

func NewSchemaManager(core Core) qdata.ModifiableSchemaManager {
	return &SchemaManager{
		core: core,
	}
}

func (me *SchemaManager) SetEntityManager(entityManager qdata.EntityManager) {
	me.entityManager = entityManager
}

func (me *SchemaManager) SetFieldOperator(fieldOperator qdata.FieldOperator) {
	me.fieldOperator = fieldOperator
}

func (me *SchemaManager) GetFieldSchema(ctx context.Context, entityType, fieldName string) qdata.FieldSchema {
	schemaPb := &qprotobufs.DatabaseFieldSchema{}
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
            SELECT field_name, field_type, read_permissions, write_permissions
            FROM EntitySchema
            WHERE entity_type = $1 AND field_name = $2
        `, entityType, fieldName).Scan(&schemaPb.Name, &schemaPb.Type, &schemaPb.ReadPermissions, &schemaPb.WritePermissions)
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
				WHERE entity_type = $1 AND field_name = $2
			`, entityType, fieldName).Scan(&options)
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

func (me *SchemaManager) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema qdata.FieldSchema) {
	// Find entity schema
	entitySchema := me.GetEntitySchema(ctx, entityType)
	if entitySchema == nil {
		qlog.Error("Failed to get entity schema for type %s", entityType)
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

func (me *SchemaManager) FieldExists(ctx context.Context, fieldName, entityType qdata.EntityType) bool {
	exists := false

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		err := tx.QueryRow(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM EntitySchema 
				WHERE entity_type = $1 AND field_name = $2
			)
		`, entityType, fieldName).Scan(&exists)
		if err != nil {
			qlog.Error("Failed to check field existence: %v", err)
		}
	})

	return exists
}

func (me *SchemaManager) SetEntitySchema(ctx context.Context, requestedSchema qdata.EntitySchema) {
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		// Get existing schema for comparison
		oldSchema := me.GetEntitySchema(ctx, requestedSchema.GetType())

		// Delete existing schema
		_, err := tx.Exec(ctx, `
			DELETE FROM EntitySchema WHERE entity_type = $1
		`, requestedSchema.GetType())
		if err != nil {
			qlog.Error("Failed to delete existing schema: %v", err)
			return
		}

		// Delete existing choice options
		_, err = tx.Exec(ctx, `
			DELETE FROM ChoiceOptions WHERE entity_type = $1
		`, requestedSchema.GetType())
		if err != nil {
			qlog.Error("Failed to delete existing choice options: %v", err)
			return
		}

		// Build new schema
		fields := []qdata.FieldSchema{}
		if !slices.ContainsFunc(requestedSchema.GetFields(), func(field qdata.FieldSchema) bool {
			return field.GetFieldName() == "Name"
		}) {
			fields = append(fields, qfield.NewSchema("Name", qfield.String))
		}

		if !slices.ContainsFunc(requestedSchema.GetFields(), func(field qdata.FieldSchema) bool {
			return field.GetFieldName() == "Description"
		}) {
			fields = append(fields, qfield.NewSchema("Description", qfield.String))
		}

		if !slices.ContainsFunc(requestedSchema.GetFields(), func(field qdata.FieldSchema) bool {
			return field.GetFieldName() == "Parent"
		}) {
			fields = append(fields, qfield.NewSchema("Parent", qfield.EntityReference))
		}

		if !slices.ContainsFunc(requestedSchema.GetFields(), func(field qdata.FieldSchema) bool {
			return field.GetFieldName() == "Children"
		}) {
			fields = append(fields, qfield.NewSchema("Children", qfield.EntityList))
		}

		fields = append(fields, requestedSchema.GetFields()...)
		modifiableSchema := qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{})
		modifiableSchema.SetType(requestedSchema.GetType())
		modifiableSchema.SetFields(fields)

		for i, field := range modifiableSchema.GetFields() {
			// Remove non-existant entity ids from read/write permissions
			readPermissions := []string{}
			for _, id := range field.GetReadPermissions() {
				entity := me.entityManager.GetEntity(ctx, id)
				if entity != nil && entity.GetType() == "Permission" {
					readPermissions = append(readPermissions, id)
				}
			}

			writePermissions := []string{}
			for _, id := range field.GetWritePermissions() {
				entity := me.entityManager.GetEntity(ctx, id)
				if entity != nil && entity.GetType() == "Permission" {
					writePermissions = append(writePermissions, id)
				}
			}

			_, err = tx.Exec(ctx, `
            INSERT INTO EntitySchema (entity_type, field_name, field_type, read_permissions, write_permissions, rank)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (entity_type, field_name) 
            DO UPDATE SET field_type = $3, read_permissions = $4, write_permissions = $5, rank = $6
        `, modifiableSchema.GetType(), field.GetFieldName(), field.GetFieldType(), readPermissions, writePermissions, i)

			if err != nil {
				qlog.Error("Failed to set field schema: %v", err)
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
            `, modifiableSchema.GetType(), field.GetFieldName(), options)

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
			for _, oldField := range oldSchema.GetFields() {
				found := false
				for _, newField := range modifiableSchema.GetFields() {
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
			for _, newField := range modifiableSchema.GetFields() {
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
			entities := me.entityManager.FindEntities(ctx, modifiableSchema.GetType())
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
						qlog.Error("Failed to delete field: %v", err)
						continue
					}
				}

				// Initialize new fields
				for _, fieldName := range newFields {
					req := qrequest.New().SetEntityId(entityId).SetFieldName(fieldName)
					me.fieldOperator.Write(ctx, req)
				}
			}
		}
	})
}

func (me *SchemaManager) GetEntitySchema(ctx context.Context, entityType string) qdata.ModifiableEntitySchema {
	schema := qentity.FromSchemaPb(&qprotobufs.DatabaseEntitySchema{})
	schema.SetType(entityType)
	var fields []qdata.FieldSchema

	type FieldRow struct {
		FieldName        string
		FieldType        string
		Rank             int
		ReadPermissions  []string
		WritePermissions []string
	}

	var fieldRows []FieldRow

	err := BatchedQuery(me.core, ctx, `
		SELECT field_name, field_type, read_permissions, write_permissions, rank, cursor_id
		FROM EntitySchema
		WHERE entity_type = $1
	`,
		[]any{entityType},
		0, // use default batch size
		func(rows pgx.Rows, cursorId *int64) (FieldRow, error) {
			var fr FieldRow
			err := rows.Scan(&fr.FieldName, &fr.FieldType, &fr.ReadPermissions, &fr.WritePermissions, &fr.Rank, cursorId)
			return fr, err
		},
		func(batch []FieldRow) error {
			fieldRows = append(fieldRows, batch...)
			return nil
		})

	if err != nil {
		qlog.Error("Failed to get entity schema: %v", err)
	}

	// Sort field rows by rank
	slices.SortFunc(fieldRows, func(a, b FieldRow) int {
		return a.Rank - b.Rank
	})

	// Process the fields in sorted order
	for _, fr := range fieldRows {
		fieldSchema := qfield.FromSchemaPb(&qprotobufs.DatabaseFieldSchema{
			Name:             fr.FieldName,
			Type:             fr.FieldType,
			ReadPermissions:  fr.ReadPermissions,
			WritePermissions: fr.WritePermissions,
		})

		// If it's a choice field, get the options
		if fieldSchema.IsChoice() {
			var options []string
			me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
				err := tx.QueryRow(ctx, `
					SELECT options
					FROM ChoiceOptions
					WHERE entity_type = $1 AND field_name = $2
				`, entityType, fr.FieldName).Scan(&options)
				if err != nil {
					if !errors.Is(err, pgx.ErrNoRows) {
						qlog.Error("Failed to get choice options: %v", err)
					}
				} else {
					fieldSchema.AsChoiceFieldSchema().SetChoices(options)
				}
			})
		}

		fields = append(fields, fieldSchema)
	}

	schema.SetFields(fields)
	return schema
}
