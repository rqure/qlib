package qdata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/xwb1989/sqlparser"
)

type QueryField struct {
	Name       string
	IsMetadata bool
	MetaType   string // WriterId, WriteTime, EntityType
	Target     string // For indirection, stores the target field
	Alias      string
}

type QueryTable struct {
	EntityType string
	Alias      string
}

type ParsedQuery struct {
	Fields      []QueryField
	Table       QueryTable
	Where       *sqlparser.Where
	OrderBy     sqlparser.OrderBy
	Limit       *sqlparser.Limit
	OriginalSQL string
}

func ParseQuery(sql string) (*ParsedQuery, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %v", err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("only SELECT statements are supported")
	}

	if len(selectStmt.From) != 1 {
		return nil, fmt.Errorf("exactly one FROM table must be specified")
	}

	parsed := &ParsedQuery{
		Fields:      make([]QueryField, 0),
		Where:       selectStmt.Where,
		OrderBy:     selectStmt.OrderBy,
		Limit:       selectStmt.Limit,
		OriginalSQL: sql,
	}

	// Parse table
	tableExpr := selectStmt.From[0]
	if aliasedTable, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
		tableName := sqlparser.String(aliasedTable.Expr)
		parsed.Table = QueryTable{
			EntityType: strings.Trim(tableName, "`"),
			Alias:      aliasedTable.As.String(), // Use String() method instead of direct conversion
		}
	}

	// Parse fields
	for _, expr := range selectStmt.SelectExprs {
		field, err := parseSelectExpr(expr)
		if err != nil {
			return nil, err
		}
		parsed.Fields = append(parsed.Fields, field)
	}

	return parsed, nil
}

func parseSelectExpr(expr sqlparser.SelectExpr) (QueryField, error) {
	aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return QueryField{}, fmt.Errorf("unsupported select expression type")
	}

	field := QueryField{
		Alias: aliasedExpr.As.String(), // Use String() method instead of direct conversion
	}

	// Check for metadata fields: WriterId(field), WriteTime(field), EntityType(field)
	if funcExpr, ok := aliasedExpr.Expr.(*sqlparser.FuncExpr); ok {
		field.IsMetadata = true
		field.MetaType = sqlparser.String(funcExpr.Name)
		if len(funcExpr.Exprs) > 0 {
			field.Name = sqlparser.String(funcExpr.Exprs[0])
		}
		return field, nil
	}

	// Check for field indirection (->)
	colName := sqlparser.String(aliasedExpr.Expr)
	parts := strings.Split(colName, "->")
	if len(parts) > 1 {
		field.Name = strings.TrimSpace(parts[0])
		field.Target = strings.TrimSpace(parts[1])
	} else {
		field.Name = colName
	}

	return field, nil
}

type SQLiteBuilder struct {
	db    *sql.DB
	store StoreInteractor
}

func NewSQLiteBuilder(store StoreInteractor) (*SQLiteBuilder, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	return &SQLiteBuilder{
		db:    db,
		store: store,
	}, nil
}

func (me *SQLiteBuilder) BuildTable(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	// Get entity schema
	schema := me.store.GetEntitySchema(ctx, entityType)
	if schema == nil {
		return fmt.Errorf("schema not found for entity type: %s", entityType)
	}

	// Create table with all necessary columns
	columns := make([]string, 0)
	columns = append(columns, "id TEXT PRIMARY KEY")

	for _, field := range schema.Fields {
		colType := getSQLiteType(field.ValueType)
		if colType != "" {
			columns = append(columns, fmt.Sprintf("%s %s", field.FieldType, colType))
			// Add metadata columns if needed
			columns = append(columns, fmt.Sprintf("%s_writer_id TEXT", field.FieldType))
			columns = append(columns, fmt.Sprintf("%s_write_time DATETIME", field.FieldType))
		}
	}

	createSQL := fmt.Sprintf("CREATE TABLE entities (%s)", strings.Join(columns, ", "))
	if _, err := me.db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Populate table with data
	return me.populateTable(ctx, entityType, schema)
}

func (me *SQLiteBuilder) populateTable(ctx context.Context, entityType EntityType, schema *EntitySchema) error {
	// Get all entities of this type
	entityIterator := me.store.FindEntities(entityType)

	for entityIterator.Next(ctx) {
		entityId := entityIterator.Get()

		// Prepare insert statement with placeholders
		cols := []string{"id"}
		vals := []string{"?"}
		args := []interface{}{entityId}

		// Read all fields for this entity
		for fieldType := range schema.Fields {
			req := new(Request).Init(entityId, fieldType)
			me.store.Read(ctx, req)

			if req.Success {
				cols = append(cols, fieldType.AsString())
				vals = append(vals, "?")
				args = append(args, convertValueForSQLite(req.Value))

				// Add metadata
				cols = append(cols, fmt.Sprintf("%s_writer_id", fieldType))
				vals = append(vals, "?")
				args = append(args, req.WriterId)

				cols = append(cols, fmt.Sprintf("%s_write_time", fieldType))
				vals = append(vals, "?")
				args = append(args, req.WriteTime.AsTime())
			}
		}

		insertSQL := fmt.Sprintf("INSERT INTO entities (%s) VALUES (%s)",
			strings.Join(cols, ", "),
			strings.Join(vals, ", "))

		if _, err := me.db.ExecContext(ctx, insertSQL, args...); err != nil {
			return fmt.Errorf("failed to insert entity data: %v", err)
		}
	}

	return nil
}

func getSQLiteType(valueType ValueType) string {
	switch valueType {
	case VTInt, VTChoice:
		return "INTEGER"
	case VTFloat:
		return "REAL"
	case VTString, VTBinaryFile, VTEntityReference:
		return "TEXT"
	case VTBool:
		return "INTEGER"
	case VTTimestamp:
		return "DATETIME"
	case VTEntityList:
		return "TEXT"
	default:
		return ""
	}
}

func convertValueForSQLite(value *Value) interface{} {
	switch {
	case value.IsInt():
		return value.GetInt()
	case value.IsFloat():
		return value.GetFloat()
	case value.IsString():
		return value.GetString()
	case value.IsBool():
		return value.GetBool()
	case value.IsBinaryFile():
		return value.GetBinaryFile()
	case value.IsEntityReference():
		return string(value.GetEntityReference())
	case value.IsTimestamp():
		return value.GetTimestamp()
	case value.IsChoice():
		return value.GetChoice()
	case value.IsEntityList():
		// Convert entity list to JSON string
		ids := value.GetEntityList()
		strIds := make([]string, len(ids))
		for i, id := range ids {
			strIds[i] = string(id)
		}
		return "[" + strings.Join(strIds, ",") + "]"
	default:
		return nil
	}
}

func (sb *SQLiteBuilder) ExecuteQuery(ctx context.Context, query *ParsedQuery) (*sql.Rows, error) {
	// Build the SELECT clause
	selectFields := make([]string, len(query.Fields))
	for i, field := range query.Fields {
		if field.IsMetadata {
			switch field.MetaType {
			case "WriterId":
				selectFields[i] = fmt.Sprintf("%s_writer_id as %s", field.Name, field.Alias)
			case "WriteTime":
				selectFields[i] = fmt.Sprintf("%s_write_time as %s", field.Name, field.Alias)
			case "EntityType":
				selectFields[i] = "(SELECT type FROM Entities WHERE id = entities.id) as " + field.Alias
			}
		} else if field.Target != "" {
			// Handle field indirection
			selectFields[i] = fmt.Sprintf("(SELECT field_value FROM entities WHERE id = %s) as %s",
				field.Target, field.Alias)
		} else {
			selectFields[i] = fmt.Sprintf("%s as %s", field.Name, field.Alias)
		}
	}

	// Build the complete query
	sqlQuery := fmt.Sprintf("SELECT id, %s FROM entities", strings.Join(selectFields, ", "))

	if query.Where != nil {
		sqlQuery += " WHERE " + sqlparser.String(query.Where)
	}

	if len(query.OrderBy) > 0 {
		sqlQuery += " ORDER BY " + sqlparser.String(query.OrderBy)
	}

	if query.Limit != nil {
		sqlQuery += " LIMIT " + sqlparser.String(query.Limit)
	}

	// Execute the query
	return sb.db.QueryContext(ctx, sqlQuery)
}

func (sb *SQLiteBuilder) RowToEntity(ctx context.Context, rows *sql.Rows, query *ParsedQuery, schemaCache map[EntityType]*EntitySchema) (*Entity, error) {
	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Scan the row into the values slice
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %v", err)
	}

	// First column should be the ID
	entityId := EntityId(values[0].(string))
	entityType := EntityType(query.Table.EntityType)

	// Create the entity
	entity := new(Entity).Init(entityType, entityId)

	// Process each field
	for i, field := range query.Fields {
		fieldType := FieldType(field.Name)

		value := values[i+1] // +1 because first column is ID
		if value == nil {
			continue
		}

		// Convert the value based on the field schema
		if schemaCache[entityType] == nil {
			schema := sb.store.GetEntitySchema(ctx, entityType)
			if schema == nil {
				continue
			}
			schemaCache[entityType] = schema
		}

		schema := schemaCache[entityType]

		fieldValue := schema.Field(fieldType).ValueType.NewValue(value)
		if fieldValue == nil {
			continue
		}

		// Add the field to the entity
		entity.Field(fieldType, FOValue(fieldValue))
	}

	return entity, nil
}
