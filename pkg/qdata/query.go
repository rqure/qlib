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
	FieldType  FieldType
	IsMetadata bool
	MetaType   string // WriterId, WriteTime, EntityType
	Alias      string
}

func (me *QueryField) ColumnName() string {
	return strings.ReplaceAll(me.FieldType.AsString(), "->", "_via_")
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
			field.FieldType.FromString(sqlparser.String(funcExpr.Exprs[0]))
		}
		return field, nil
	}

	colName := sqlparser.String(aliasedExpr.Expr)
	field.FieldType.FromString(colName)

	return field, nil
}

type SQLiteBuilder struct {
	db          *sql.DB
	store       StoreInteractor
	entityCache map[EntityId]*Entity // Cache entities by ID

}

func NewSQLiteBuilder(store StoreInteractor) (*SQLiteBuilder, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}

	return &SQLiteBuilder{
		db:          db,
		store:       store,
		entityCache: make(map[EntityId]*Entity),
	}, nil
}

// GetEntityFromCache retrieves an entity from cache or loads it from the store
func (me *SQLiteBuilder) GetEntityFromCache(ctx context.Context, entityId EntityId) *Entity {
	if entity, found := me.entityCache[entityId]; found {
		return entity
	}

	entity := me.store.GetEntity(ctx, entityId)
	if entity != nil {
		me.entityCache[entityId] = entity
	}
	return entity
}

func (me *SQLiteBuilder) BuildTable(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	// Create table with all necessary columns
	columns := make([]string, 0)
	columns = append(columns, "id TEXT PRIMARY KEY")

	for _, field := range query.Fields {
		var colType string
		if field.FieldType.IsIndirection() {

		} else {
			schema := me.store.GetFieldSchema(ctx, entityType, field.FieldType)
			colType = getSQLiteType(schema.ValueType)
		}
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

	// Create indexes for better performance
	_, err := me.db.ExecContext(ctx, "CREATE INDEX idx_entities_id ON entities(id)")
	if err != nil {
		return fmt.Errorf("failed to create index: %v", err)
	}

	return nil
}

// PopulateTableBatch loads entity data in batches and populates the SQLite table
func (me *SQLiteBuilder) PopulateTableBatch(ctx context.Context, entityType EntityType, query *ParsedQuery, pageSize int64, cursorId int64) (int64, bool, error) {
	// Get entities in batches
	pageOpts := []PageOpts{POPageSize(pageSize), POCursorId(cursorId)}
	entityIterator := me.store.FindEntities(entityType, pageOpts...)

	pageResult, err := entityIterator.NextPage(ctx)
	if err != nil {
		return cursorId, false, fmt.Errorf("failed to get entities: %v", err)
	}

	// Begin a transaction for batch inserts
	tx, err := me.db.BeginTx(ctx, nil)
	if err != nil {
		return cursorId, false, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Prepare the insert statement once
	stmt, err := tx.PrepareContext(ctx, "INSERT OR IGNORE INTO entities (id) VALUES (?)")
	if err != nil {
		return cursorId, false, fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Process all entities in this batch
	count := 0
	entityIds := make([]EntityId, 0, pageSize)

	for _, entityId := range pageResult.Items {
		count++
		entityIds = append(entityIds, entityId)

		// Insert the entity ID first
		if _, err := stmt.ExecContext(ctx, entityId); err != nil {
			return cursorId, false, fmt.Errorf("failed to insert entity: %v", err)
		}
	}

	// Now load field data in bulk for all entities in this batch
	if len(entityIds) > 0 {
		if err := me.loadQueryFieldsBulk(ctx, entityIds, query); err != nil {
			return cursorId, false, err
		}
	}

	// For PostgreSQL-based stores, the pageConfig.CursorId is updated in the NextPage function
	// We can safely pass back the cursorId we were given since the store has tracked the next cursor internally
	return cursorId, pageResult.HasMore, nil
}

// loadQueryFieldsBulk loads only the fields specified in the query, handling indirection and metadata
func (me *SQLiteBuilder) loadQueryFieldsBulk(ctx context.Context, entityIds []EntityId, query *ParsedQuery) error {
	if len(entityIds) == 0 {
		return nil
	}

	// Create a set of entities for which we need to get data
	entityRequests := make(map[EntityId][]*Request)

	// First, populate the entity cache for all entities
	for _, entityId := range entityIds {
		if _, exists := me.entityCache[entityId]; !exists {
			me.entityCache[entityId] = new(Entity).Init(entityId)
		}
		entityRequests[entityId] = make([]*Request, 0)
	}

	// Determine all the fields we need to fetch
	for _, entityId := range entityIds {
		if entity, exists := me.entityCache[entityId]; exists {
			// Add each field from the query
			for _, field := range query.Fields {
				entityRequests[entityId] = append(entityRequests[entityId], entity.Field(field.FieldType).AsReadRequest())
			}
		}
	}

	// Flatten all requests for bulk reading
	allRequests := make([]*Request, 0)
	for _, requests := range entityRequests {
		allRequests = append(allRequests, requests...)
	}

	// Execute all read requests in a batch
	me.store.Read(ctx, allRequests...)

	// Process successful reads and update SQLite
	tx, err := me.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Process each entity's fields
	for entityId, requests := range entityRequests {
		for _, req := range requests {
			if !req.Success {
				continue
			}

			fieldType := req.FieldType

			// Find the corresponding field in the query
			var queryField *QueryField
			for i := range query.Fields {
				if query.Fields[i].FieldType == fieldType {
					queryField = &query.Fields[i]
					break
				}
			}

			if queryField == nil {
				continue
			}

			// Handle the field based on its type
			if queryField.IsMetadata {
				// Get the metadata for this field
				switch queryField.MetaType {
				case "WriterId":
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                        UPDATE entities SET %s_writer_id = ? WHERE id = ?
                    `, queryField.ColumnName()), req.WriterId.AsString(), entityId)
				case "WriteTime":
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                        UPDATE entities SET %s_write_time = ? WHERE id = ?
                    `, queryField.ColumnName()), req.WriteTime.AsTime(), entityId)
				case "EntityType":
					// EntityType is derived from the entity itself
					if entity, exists := me.entityCache[entityId]; exists {
						_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                            UPDATE entities SET %s = ? WHERE id = ?
                        `, queryField.ColumnName()), entity.EntityType, entityId)
					}
				}
			} else {
				// Direct field value
				_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                    UPDATE entities SET %s = ?, %s_writer_id = ?, %s_write_time = ? WHERE id = ?
                `, queryField.ColumnName(), queryField.ColumnName(), queryField.ColumnName()),
					convertValueForSQLite(req.Value),
					req.WriterId.AsString(),
					req.WriteTime.AsTime(),
					entityId)
			}

			if err != nil {
				return fmt.Errorf("failed to update entity field: %v", err)
			}
		}
	}

	return tx.Commit()
}

// ExecuteQuery now supports pagination and handles all field types properly
func (sb *SQLiteBuilder) ExecuteQuery(ctx context.Context, query *ParsedQuery, limit int64, offset int64) (*sql.Rows, error) {
	// Build the SELECT clause
	selectFields := make([]string, len(query.Fields))
	for i, field := range query.Fields {
		if field.IsMetadata {
			switch field.MetaType {
			case "WriterId":
				selectFields[i] = fmt.Sprintf("%s_writer_id as %s", field.ColumnName(), field.Alias)
			case "WriteTime":
				selectFields[i] = fmt.Sprintf("%s_write_time as %s", field.ColumnName(), field.Alias)
			case "EntityType":
				selectFields[i] = "(SELECT type FROM Entities WHERE id = entities.id) as " + field.Alias
			}
		} else {
			selectFields[i] = fmt.Sprintf("%s as %s", field.ColumnName(), field.Alias)
		}
	}

	// Build the complete query
	sqlQuery := fmt.Sprintf("SELECT id, %s FROM entities", strings.Join(selectFields, ", "))

	if query.Where != nil {
		sqlQuery += " WHERE " + sqlparser.String(query.Where)
	}

	if len(query.OrderBy) > 0 {
		sqlQuery += " ORDER BY " + sqlparser.String(query.OrderBy)
	} else {
		// Default ordering by ID if none specified
		sqlQuery += " ORDER BY id"
	}

	// Apply pagination
	sqlQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	// Execute the query
	return sb.db.QueryContext(ctx, sqlQuery)
}

// QueryWithPagination executes the query with pagination and returns a PageResult
func (sb *SQLiteBuilder) QueryWithPagination(ctx context.Context, entityType EntityType, query *ParsedQuery, pageSize int64, cursorId int64) (*PageResult[*Entity], error) {
	// Create the SQLite table with the appropriate schema
	if err := sb.BuildTable(ctx, entityType, query); err != nil {
		return nil, fmt.Errorf("failed to build SQLite table: %v", err)
	}

	// Load data in batches until we have enough for this page
	var hasMore bool
	var err error

	// First batch load with the updated method that handles field loading properly
	cursorId, hasMore, err = sb.PopulateTableBatch(ctx, entityType, query, pageSize, cursorId)
	if err != nil {
		return nil, fmt.Errorf("failed to populate table: %v", err)
	}

	// Execute query with pagination parameters
	rows, err := sb.ExecuteQuery(ctx, query, pageSize, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Convert results to entities, using our cache
	var entities []*Entity

	for rows.Next() {
		entity, err := sb.RowToEntity(ctx, rows, query)
		if err != nil {
			return nil, fmt.Errorf("failed to convert row to entity: %v", err)
		}
		entities = append(entities, entity)
	}

	// Create PageResult with next page function
	return &PageResult[*Entity]{
		Items:   entities,
		HasMore: hasMore,
		NextPage: func(ctx context.Context) (*PageResult[*Entity], error) {
			return sb.QueryWithPagination(ctx, entityType, query, pageSize, cursorId)
		},
	}, nil
}

// RowToEntity now uses the entity and schema caches
func (sb *SQLiteBuilder) RowToEntity(ctx context.Context, rows *sql.Rows, query *ParsedQuery) (*Entity, error) {
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

	// Check if entity is in cache first
	entity := sb.GetEntityFromCache(ctx, entityId)
	if entity == nil {
		return nil, fmt.Errorf("entity not found in cache: %s", entityId)
	}

	return entity, nil
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
		return strings.Join(CastEntityIdSliceToStringSlice(value.GetEntityList()), ",")
	default:
		return nil
	}
}
