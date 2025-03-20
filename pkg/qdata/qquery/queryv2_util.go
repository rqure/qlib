package qquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/rqure/qlib/pkg/qdata/qfield"
)

func (q *QueryV2) sanitizeColumnName(name string) string {
	// Convert field indirection syntax to valid SQLite column name
	// Example: "NextStation->Name" becomes "NextStation_Name"
	return strings.ReplaceAll(name, "->", "_")
}

func (q *QueryV2) ensureColumns(ctx context.Context, columnMap map[string]struct{}) error {
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	entitySchema := q.store.GetEntitySchema(ctx, q.entityType)
	if entitySchema == nil {
		return fmt.Errorf("entity type %s not found", q.entityType)
	}

	for fieldName := range columnMap {
		if fieldName == "id" {
			continue
		}

		sqlType := "TEXT" // default type
		fieldSchema := entitySchema.GetField(fieldName)
		if fieldSchema == nil {
			return fmt.Errorf("field %s not found in schema", fieldName)
		}

		sqlType = q.getSQLiteType(fieldSchema.GetFieldType())
		columnName := q.sanitizeColumnName(fieldName)

		// Add columns for field value, writer, and write_time
		_, err = tx.Exec(fmt.Sprintf(
			`ALTER TABLE entities ADD COLUMN IF NOT EXISTS %s %s;
			 ALTER TABLE entities ADD COLUMN IF NOT EXISTS %s_writer TEXT;
			 ALTER TABLE entities ADD COLUMN IF NOT EXISTS %s_write_time INTEGER;`,
			columnName, sqlType, columnName, columnName),
		)
		if err != nil {
			return fmt.Errorf("failed to add columns for field %s: %v", fieldName, err)
		}
	}

	return tx.Commit()
}

func (q *QueryV2) getSQLiteType(fieldType string) string {
	switch fieldType {
	case qfield.Int:
		return "INTEGER"
	case qfield.Float:
		return "REAL"
	case qfield.Bool:
		return "INTEGER" // SQLite has no boolean, use INTEGER 0/1
	case qfield.Timestamp:
		return "INTEGER" // Store as Unix timestamp
	case qfield.EntityReference:
		return "TEXT" // Store entity ID as text
	case qfield.EntityList:
		return "TEXT" // Store as comma-separated list
	case qfield.Choice:
		return "INTEGER" // Store choice index
	default:
		return "TEXT"
	}
}
