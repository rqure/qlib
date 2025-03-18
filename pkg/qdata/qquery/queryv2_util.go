package qquery

import (
	"context"
	"database/sql"
	"strings"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
)

func (q *QueryV2) processSQL(sql string) string {
	// Replace field indirection syntax with valid SQLite column names
	parts := strings.Split(sql, " ")
	for i, part := range parts {
		if strings.Contains(part, "->") {
			parts[i] = q.sanitizeColumnName(part)
		}
	}
	return strings.Join(parts, " ")
}

func (q *QueryV2) sanitizeColumnName(name string) string {
	// Convert field indirection syntax to valid SQLite column name
	// Example: "NextStation->Name" becomes "NextStation_Name"
	return strings.ReplaceAll(name, "->", "_")
}

func (q *QueryV2) getFieldValue(field qdata.FieldBinding) interface{} {
	if field == nil {
		return nil
	}

	value := field.GetValue()
	if value == nil {
		return nil
	}

	switch {
	case value.IsString():
		return value.GetString()
	case value.IsInt():
		return value.GetInt()
	case value.IsFloat():
		return value.GetFloat()
	case value.IsBool():
		return value.GetBool()
	case value.IsTimestamp():
		return value.GetTimestamp().Unix()
	case value.IsEntityReference():
		return value.GetEntityReference()
	case value.IsBinaryFile():
		return value.GetBinaryFile()
	default:
		return nil
	}
}

func (q *QueryV2) processResults(ctx context.Context, rows *sql.Rows) []qdata.EntityBinding {
	columns, err := rows.Columns()
	if err != nil {
		return nil
	}

	var results []qdata.EntityBinding
	multi := qbinding.NewMulti(q.store)

	// Prepare value holders
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Process each row
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		// Get entity ID (first column)
		if id, ok := values[0].(string); ok {
			entity := qbinding.NewEntity(ctx, q.store, id)

			// Set field values from query results
			for i := 1; i < len(columns); i++ {
				if values[i] == nil {
					continue
				}

				fieldName := columns[i]
				// Convert SQLite column name back to indirection syntax
				fieldName = strings.ReplaceAll(fieldName, "_", "->")

				// Let the entity system handle field resolution
				field := entity.GetField(fieldName)
				if field != nil {
					field.ReadValue(ctx)
				}
			}

			results = append(results, entity)
		}
	}

	multi.Commit(ctx)
	return results
}
