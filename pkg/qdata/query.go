package qdata

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqure/qlib/pkg/qlog"
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

type TypeHintMap map[FieldType]ValueType
type TypeHintOpts func(TypeHintMap)

func TypeHint(ft FieldType, vt ValueType) TypeHintOpts {
	return func(m TypeHintMap) {
		m[ft] = vt
	}
}

func (me TypeHintMap) ApplyOpts(opts ...TypeHintOpts) TypeHintMap {
	for _, opt := range opts {
		opt(me)
	}

	return me
}

func ParseQuery(sql string) (*ParsedQuery, error) {
	qlog.Trace("ParseQuery: Parsing SQL: %s", sql)
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		qlog.Trace("ParseQuery: Failed to parse SQL: %v", err)
		return nil, fmt.Errorf("failed to parse SQL: %v", err)
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		qlog.Trace("ParseQuery: Only SELECT statements are supported")
		return nil, fmt.Errorf("only SELECT statements are supported")
	}

	if len(selectStmt.From) != 1 {
		qlog.Trace("ParseQuery: Exactly one FROM table must be specified")
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
			Alias:      aliasedTable.As.String(),
		}
		qlog.Trace("ParseQuery: Parsed table: %s with alias: %s", parsed.Table.EntityType, parsed.Table.Alias)
	}

	// Parse fields
	for _, expr := range selectStmt.SelectExprs {
		field, err := parseSelectExpr(expr)
		if err != nil {
			qlog.Trace("ParseQuery: Failed to parse field expression: %v", err)
			return nil, err
		}
		parsed.Fields = append(parsed.Fields, field)
		qlog.Trace("ParseQuery: Parsed field: %s, alias: %s, isMetadata: %t", field.FieldType.AsString(), field.Alias, field.IsMetadata)
	}

	qlog.Trace("ParseQuery: Successfully parsed query with %d fields", len(parsed.Fields))
	return parsed, nil
}

func parseSelectExpr(expr sqlparser.SelectExpr) (QueryField, error) {
	qlog.Trace("parseSelectExpr: Parsing select expression")

	aliasedExpr, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		qlog.Trace("parseSelectExpr: Unsupported select expression type")
		return QueryField{}, fmt.Errorf("unsupported select expression type")
	}

	field := QueryField{}

	// Handle alias - use the column name as default alias if not explicitly specified
	if !aliasedExpr.As.IsEmpty() {
		field.Alias = aliasedExpr.As.String()
		qlog.Trace("parseSelectExpr: Found explicit alias: %s", field.Alias)
	}

	// Check for metadata fields: WriterId(field), WriteTime(field), EntityType(field)
	if funcExpr, ok := aliasedExpr.Expr.(*sqlparser.FuncExpr); ok {
		field.IsMetadata = true
		field.MetaType = sqlparser.String(funcExpr.Name)
		qlog.Trace("parseSelectExpr: Detected metadata field of type: %s", field.MetaType)

		if len(funcExpr.Exprs) > 0 {
			field.FieldType.FromString(sqlparser.String(funcExpr.Exprs[0]))
			qlog.Trace("parseSelectExpr: Metadata field references: %s", field.FieldType.AsString())
		}

		// If no alias is specified for a metadata field, create a default one
		if field.Alias == "" {
			field.Alias = fmt.Sprintf("%s_%s", field.MetaType, field.FieldType.AsString())
			qlog.Trace("parseSelectExpr: Created default metadata alias: %s", field.Alias)
		}

		return field, nil
	}

	// For regular fields
	colName := sqlparser.String(aliasedExpr.Expr)
	field.FieldType.FromString(colName)
	qlog.Trace("parseSelectExpr: Regular field with type: %s", field.FieldType.AsString())

	// If no alias is specified, use the sanitized field name as alias
	if field.Alias == "" {
		field.Alias = field.ColumnName() // Use ColumnName() which handles -> replacement
		qlog.Trace("parseSelectExpr: Created default alias from field name: %s", field.Alias)
	}

	return field, nil
}

type SQLiteBuilder struct {
	db          *sql.DB
	store       StoreInteractor
	entityCache map[EntityId]*Entity // Cache entities by ID
	typeHints   TypeHintMap
	tableName   string // Unique table name for this builder
	closed      bool   // Track if the builder has been closed
}

// Static counter for generating unique table names
var tableCounter int64 = 0

func NewSQLiteBuilder(store StoreInteractor) (*SQLiteBuilder, error) {
	qlog.Trace("NewSQLiteBuilder: Creating new SQLite builder")
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		qlog.Trace("NewSQLiteBuilder: Failed to open in-memory SQLite database: %v", err)
		return nil, err
	}

	// Generate a unique table name using atomic counter
	uniqueID := atomic.AddInt64(&tableCounter, 1)
	tableName := fmt.Sprintf("entities_%d", uniqueID)

	qlog.Trace("NewSQLiteBuilder: Successfully created SQLite in-memory database with table name: %s", tableName)
	return &SQLiteBuilder{
		db:          db,
		store:       store,
		entityCache: make(map[EntityId]*Entity),
		typeHints:   make(TypeHintMap),
		tableName:   tableName,
	}, nil
}

// Close releases resources used by the SQLiteBuilder
func (me *SQLiteBuilder) Close() error {
	if me.closed {
		return nil
	}

	qlog.Trace("SQLiteBuilder.Close: Cleaning up resources")

	// Drop the table if it exists
	if me.tableName != "" {
		_, err := me.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", me.tableName))
		if err != nil {
			qlog.Trace("SQLiteBuilder.Close: Failed to drop table %s: %v", me.tableName, err)
		} else {
			qlog.Trace("SQLiteBuilder.Close: Dropped table %s", me.tableName)
		}
	}

	// Close the database connection
	err := me.db.Close()
	if err != nil {
		qlog.Trace("SQLiteBuilder.Close: Error closing database: %v", err)
		return err
	}

	// Clear the cache and mark as closed
	me.entityCache = nil
	me.closed = true
	qlog.Trace("SQLiteBuilder.Close: Resources cleaned up successfully")
	return nil
}

func (me *SQLiteBuilder) GetEntityFromCache(ctx context.Context, entityId EntityId) *Entity {
	qlog.Trace("GetEntityFromCache: Looking up entity: %s", entityId)

	if entity, found := me.entityCache[entityId]; found {
		qlog.Trace("GetEntityFromCache: Found entity in cache: %s", entityId)
		return entity
	}

	qlog.Trace("GetEntityFromCache: Entity not in cache, loading from store: %s", entityId)
	entity := me.store.GetEntity(ctx, entityId)
	if entity != nil {
		qlog.Trace("GetEntityFromCache: Successfully loaded entity from store: %s", entityId)
		me.entityCache[entityId] = entity
	} else {
		qlog.Trace("GetEntityFromCache: Entity not found in store: %s", entityId)
	}
	return entity
}

func (me *SQLiteBuilder) BuildTable(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	qlog.Trace("Building SQLite table for entity type: %s with name: %s", entityType, me.tableName)

	// Drop the table if it exists - using ExecContext directly on the db connection
	_, err := me.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", me.tableName))
	if err != nil {
		qlog.Trace("BuildTable: Failed to drop existing table: %v", err)
		return fmt.Errorf("failed to drop existing table: %v", err)
	}

	// Create table with all necessary columns
	columns := make([]string, 0)
	columns = append(columns, "id TEXT PRIMARY KEY")

	for _, field := range query.Fields {
		var colType string
		if field.FieldType.IsIndirection() {
			if vt, ok := me.typeHints[field.FieldType]; ok {
				colType = getSQLiteType(vt)
			} else {
				colType = "TEXT"
			}
		} else {
			if vt, ok := me.typeHints[field.FieldType]; ok {
				colType = getSQLiteType(vt)
			} else {
				schema := me.store.GetFieldSchema(ctx, entityType, field.FieldType)
				colType = getSQLiteType(schema.ValueType)
				me.typeHints[field.FieldType] = schema.ValueType
			}
		}
		if colType != "" {
			columns = append(columns, fmt.Sprintf("%s %s", field.ColumnName(), colType))
			// Add metadata columns if needed
			columns = append(columns, fmt.Sprintf("%s_writer_id TEXT", field.ColumnName()))
			columns = append(columns, fmt.Sprintf("%s_write_time DATETIME", field.ColumnName()))
		}
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", me.tableName, strings.Join(columns, ", "))
	qlog.Trace("Creating table with SQL: %s", createSQL)

	// Execute directly on the db connection
	_, err = me.db.ExecContext(ctx, createSQL)
	if err != nil {
		qlog.Trace("BuildTable: Failed to create table: %v", err)
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Verify the table was created by running a simple query
	var count int
	err = me.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name='%s'", me.tableName)).Scan(&count)
	if err != nil {
		qlog.Trace("BuildTable: Failed to verify table creation: %v", err)
		return fmt.Errorf("failed to verify table creation: %v", err)
	}

	if count != 1 {
		qlog.Trace("BuildTable: Table was not created successfully")
		return fmt.Errorf("table was not created successfully")
	}

	qlog.Trace("Table created successfully")
	return nil
}

func (me *SQLiteBuilder) PopulateTableBatch(ctx context.Context, entityType EntityType, query *ParsedQuery, pageSize int64, cursorId int64) (int64, error) {
	qlog.Trace("Populating table batch for entity type: %s, pageSize: %d, cursorId: %d", entityType, pageSize, cursorId)

	// Verify the table exists before proceeding
	var count int
	err := me.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name='%s'", me.tableName)).Scan(&count)
	if err != nil {
		qlog.Error("PopulateTableBatch: Failed to verify table existence: %v", err)
		return cursorId, fmt.Errorf("failed to verify table existence: %v", err)
	}

	if count != 1 {
		qlog.Error("PopulateTableBatch: Table %s does not exist", me.tableName)
		return cursorId, fmt.Errorf("table %s does not exist", me.tableName)
	}

	// Get entities in batches
	pageOpts := []PageOpts{POPageSize(pageSize), POCursorId(cursorId)}
	entityIterator := me.store.FindEntities(entityType, pageOpts...)

	pageResult, err := entityIterator.NextPage(ctx)
	if err != nil {
		qlog.Error("Failed to get entities: %v", err)
		return cursorId, fmt.Errorf("failed to get entities: %v", err)
	}
	qlog.Trace("Got page result with %d entities", len(pageResult.Items))

	// Begin a transaction for batch inserts
	tx, err := me.db.BeginTx(ctx, nil)
	if err != nil {
		return cursorId, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Prepare the insert statement once
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT OR IGNORE INTO %s (id) VALUES (?)", me.tableName))
	if err != nil {
		qlog.Error("PopulateTableBatch: Failed to prepare statement: %v", err)
		return cursorId, fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Process all entities in this batch
	entityIds := make([]EntityId, 0, len(pageResult.Items))
	for _, entityId := range pageResult.Items {
		entityIds = append(entityIds, entityId)
		qlog.Trace("Processing entity: %s", entityId)

		// Insert the entity ID first
		if _, err := stmt.ExecContext(ctx, entityId); err != nil {
			qlog.Error("PopulateTableBatch: Failed to insert entity %s: %v", entityId, err)
			return cursorId, fmt.Errorf("failed to insert entity: %v", err)
		}
	}

	// Commit this transaction to ensure IDs are saved
	if err = tx.Commit(); err != nil {
		qlog.Error("PopulateTableBatch: Failed to commit entity IDs: %v", err)
		return cursorId, fmt.Errorf("failed to commit entity IDs: %v", err)
	}

	qlog.Trace("Batch insert complete, loading field data for %d entities", len(entityIds))
	// Now load field data in bulk for all entities in this batch
	if len(entityIds) > 0 {
		if err := me.loadQueryFieldsBulk(ctx, entityIds, query); err != nil {
			return cursorId, err
		}
	}

	// Return the next cursor ID
	return pageResult.CursorId, nil
}

func (me *SQLiteBuilder) loadQueryFieldsBulk(ctx context.Context, entityIds []EntityId, query *ParsedQuery) error {
	qlog.Trace("loadQueryFieldsBulk: Loading fields for %d entities", len(entityIds))
	if len(entityIds) == 0 {
		qlog.Trace("loadQueryFieldsBulk: No entities to load")
		return nil
	}

	// Verify the table exists before proceeding
	var count int
	err := me.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name='%s'", me.tableName)).Scan(&count)
	if err != nil {
		qlog.Error("loadQueryFieldsBulk: Failed to verify table existence: %v", err)
		return fmt.Errorf("failed to verify table existence: %v", err)
	}

	if count != 1 {
		qlog.Error("loadQueryFieldsBulk: Table %s does not exist", me.tableName)
		return fmt.Errorf("table %s does not exist", me.tableName)
	}

	// Create a set of entities for which we need to get data
	entityRequests := make(map[EntityId][]*Request)

	// First, populate the entity cache for all entities
	for _, entityId := range entityIds {
		if _, exists := me.entityCache[entityId]; !exists {
			qlog.Trace("loadQueryFieldsBulk: Creating stub entity in cache: %s", entityId)
			me.entityCache[entityId] = new(Entity).Init(entityId)
		}
		entityRequests[entityId] = make([]*Request, 0)
	}

	// Determine all the fields we need to fetch
	for _, entityId := range entityIds {
		if entity, exists := me.entityCache[entityId]; exists {
			// Add each field from the query
			for _, field := range query.Fields {
				qlog.Trace("loadQueryFieldsBulk: Adding read request for entity %s, field %s",
					entityId, field.FieldType.AsString())
				entityRequests[entityId] = append(entityRequests[entityId],
					entity.Field(field.FieldType).AsReadRequest())
			}
		}
	}

	// Flatten all requests for bulk reading
	allRequests := make([]*Request, 0)
	for _, requests := range entityRequests {
		allRequests = append(allRequests, requests...)
	}
	qlog.Trace("loadQueryFieldsBulk: Batch reading %d field requests", len(allRequests))

	// Execute all read requests in a batch
	me.store.Read(ctx, allRequests...)
	qlog.Trace("loadQueryFieldsBulk: Completed batch read operation")

	// Begin a new transaction for database updates
	tx, err := me.db.BeginTx(ctx, nil)
	if err != nil {
		qlog.Error("loadQueryFieldsBulk: Failed to begin transaction: %v", err)
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	defer func() {
		if err != nil {
			qlog.Trace("loadQueryFieldsBulk: Rolling back transaction due to error")
			tx.Rollback()
		}
	}()

	// Track statistics
	successCount := 0
	failCount := 0

	// Process each entity's fields
	for entityId, requests := range entityRequests {
		for _, req := range requests {
			if !req.Success {
				failCount++
				qlog.Trace("loadQueryFieldsBulk: Request failed for entity %s, field %s",
					entityId, req.FieldType.AsString())
				continue
			}
			successCount++

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
				qlog.Trace("loadQueryFieldsBulk: No matching query field for %s", fieldType.AsString())
				continue
			}

			// Handle the field based on its type
			if queryField.IsMetadata {
				// Get the metadata for this field
				switch queryField.MetaType {
				case "WriterId":
					qlog.Trace("loadQueryFieldsBulk: Updating WriterId metadata for entity %s, field %s",
						entityId, queryField.FieldType.AsString())
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                        UPDATE %s SET %s_writer_id = ? WHERE id = ?
                    `, me.tableName, queryField.ColumnName()), req.WriterId.AsString(), entityId)
				case "WriteTime":
					qlog.Trace("loadQueryFieldsBulk: Updating WriteTime metadata for entity %s, field %s",
						entityId, queryField.FieldType.AsString())
					_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                        UPDATE %s SET %s_write_time = ? WHERE id = ?
                    `, me.tableName, queryField.ColumnName()), req.WriteTime.AsTime(), entityId)
				case "EntityType":
					// EntityType is derived from the entity itself
					if entity, exists := me.entityCache[entityId]; exists {
						qlog.Trace("loadQueryFieldsBulk: Updating EntityType metadata for entity %s", entityId)
						_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                            UPDATE %s SET %s = ? WHERE id = ?
                        `, me.tableName, queryField.ColumnName()), entity.EntityType, entityId)
					}
				}
			} else {
				// Direct field value
				qlog.Trace("loadQueryFieldsBulk: Updating value for entity %s, field %s",
					entityId, queryField.FieldType.AsString())
				_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                    UPDATE %s SET %s = ?, %s_writer_id = ?, %s_write_time = ? WHERE id = ?
                `, me.tableName, queryField.ColumnName(), queryField.ColumnName(), queryField.ColumnName()),
					convertValueForSQLite(req.Value),
					req.WriterId.AsString(),
					req.WriteTime.AsTime(),
					entityId)
			}

			if err != nil {
				qlog.Trace("loadQueryFieldsBulk: SQL error updating entity %s, field %s: %v",
					entityId, queryField.FieldType.AsString(), err)
				return fmt.Errorf("failed to update entity field: %v", err)
			}
		}
	}

	qlog.Trace("loadQueryFieldsBulk: Processed field values - success: %d, failed: %d", successCount, failCount)
	err = tx.Commit()
	if err != nil {
		qlog.Trace("loadQueryFieldsBulk: Failed to commit transaction: %v", err)
		return fmt.Errorf("failed to commit transaction: %v", err)
	}
	qlog.Trace("loadQueryFieldsBulk: Successfully committed all field updates")

	return nil
}

func (sb *SQLiteBuilder) ExecuteQuery(ctx context.Context, query *ParsedQuery, limit int64, offset int64) (*sql.Rows, error) {
	qlog.Trace("ExecuteQuery: Building query with limit %d, offset %d", limit, offset)

	// Build the SELECT clause
	selectFields := make([]string, len(query.Fields))
	for i, field := range query.Fields {
		alias := field.Alias
		if alias == "" {
			// Fallback to using sanitized column name if no alias is provided
			alias = field.ColumnName()
			qlog.Trace("ExecuteQuery: Using sanitized column name as alias for field %s", field.FieldType.AsString())
		}

		// Use sanitized column names in the SQL query
		if field.IsMetadata {
			switch field.MetaType {
			case "WriterId":
				selectFields[i] = fmt.Sprintf("%s_writer_id as %s", field.ColumnName(), alias)
				qlog.Trace("ExecuteQuery: Added WriterId metadata select: %s", selectFields[i])
			case "WriteTime":
				selectFields[i] = fmt.Sprintf("%s_write_time as %s", field.ColumnName(), alias)
				qlog.Trace("ExecuteQuery: Added WriteTime metadata select: %s", selectFields[i])
			case "EntityType":
				selectFields[i] = fmt.Sprintf("(SELECT type FROM Entities WHERE id = %s.id) as %s", sb.tableName, alias)
				qlog.Trace("ExecuteQuery: Added EntityType metadata select: %s", selectFields[i])
			}
		} else {
			selectFields[i] = fmt.Sprintf("%s as %s", field.ColumnName(), alias)
			qlog.Trace("ExecuteQuery: Added regular field select: %s", selectFields[i])
		}
	}

	// Build the complete query
	sqlQuery := fmt.Sprintf("SELECT id, %s FROM %s", strings.Join(selectFields, ", "), sb.tableName)

	if query.Where != nil {
		whereClause := sqlparser.String(query.Where)
		// Fix the WHERE clause - remove the "WHERE" keyword and any leading/trailing whitespace
		whereClause = strings.TrimSpace(whereClause)
		whereClause = strings.TrimPrefix(whereClause, "where")
		whereClause = strings.TrimPrefix(whereClause, "WHERE")
		whereClause = strings.TrimSpace(whereClause)
		sqlQuery += " WHERE " + whereClause
		qlog.Trace("ExecuteQuery: Added WHERE clause: %s", whereClause)
	}

	if len(query.OrderBy) > 0 {
		orderBy := sqlparser.String(query.OrderBy)
		// Fix the ORDER BY clause - remove the "ORDER BY" keywords and any leading/trailing whitespace
		orderBy = strings.TrimSpace(orderBy)
		orderBy = strings.TrimPrefix(orderBy, "order by")
		orderBy = strings.TrimPrefix(orderBy, "ORDER BY")
		orderBy = strings.TrimSpace(orderBy)
		sqlQuery += " ORDER BY " + orderBy
		qlog.Trace("ExecuteQuery: Added ORDER BY clause: %s", orderBy)
	} else {
		// Default ordering by ID if none specified
		sqlQuery += " ORDER BY id"
		qlog.Trace("ExecuteQuery: Using default ORDER BY id")
	}

	// Apply pagination
	sqlQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	qlog.Trace("ExecuteQuery: Final query: %s", sqlQuery)

	// Execute the query
	rows, err := sb.db.QueryContext(ctx, sqlQuery)
	if err != nil {
		qlog.Error("ExecuteQuery: SQLite query failed: %v", err)
		return nil, err
	}

	qlog.Trace("ExecuteQuery: Query executed successfully")
	return rows, nil
}

func (sb *SQLiteBuilder) QueryWithPagination(ctx context.Context, entityType EntityType, query *ParsedQuery, pageSize int64, cursorId int64, opts ...TypeHintOpts) (*PageResult[*Entity], error) {
	qlog.Trace("QueryWithPagination: Starting for entity type %s, pageSize %d, cursorId %d",
		entityType, pageSize, cursorId)

	// Apply type hints
	for _, opt := range opts {
		opt(sb.typeHints)
	}
	qlog.Trace("QueryWithPagination: Applied %d type hints", len(sb.typeHints))

	// Create the SQLite table with the appropriate schema
	if err := sb.BuildTable(ctx, entityType, query); err != nil {
		qlog.Trace("QueryWithPagination: Failed to build SQLite table: %v", err)
		return nil, fmt.Errorf("failed to build SQLite table: %v", err)
	}
	qlog.Trace("QueryWithPagination: Successfully built SQLite table")

	// Set a reasonable default for page size if it's not positive
	if pageSize <= 0 {
		pageSize = 100
		qlog.Trace("QueryWithPagination: Using default page size: %d", pageSize)
	}

	// Load data in batches until we have enough for this page
	nextCursorId, err := sb.PopulateTableBatch(ctx, entityType, query, pageSize, cursorId)
	if err != nil {
		qlog.Trace("QueryWithPagination: Failed to populate table: %v", err)
		return nil, fmt.Errorf("failed to populate table: %v", err)
	}
	qlog.Trace("QueryWithPagination: Table populated. nextCursorId: %d", nextCursorId)

	// Execute query with exact pagination parameters (no need for extra items)
	rows, err := sb.ExecuteQuery(ctx, query, pageSize, 0)
	if err != nil {
		qlog.Trace("QueryWithPagination: Failed to execute query: %v", err)
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()
	qlog.Trace("QueryWithPagination: Query executed, processing results")

	// Convert results to entities, using our cache
	var entities []*Entity
	entityCount := 0
	errorCount := 0

	for rows.Next() {
		entity, err := sb.RowToEntity(ctx, rows, query)
		if err != nil {
			qlog.Error("QueryWithPagination: Failed to convert row to entity: %v", err)
			errorCount++
			continue
		}
		entities = append(entities, entity)
		entityCount++
	}

	qlog.Trace("QueryWithPagination: Processed %d entities with %d errors",
		entityCount, errorCount)

	// If we got fewer results than requested, we're at the end
	if len(entities) < int(pageSize) {
		nextCursorId = -1
	}

	// Create a reference to this builder to close it later
	builderRef := sb

	// Create PageResult with next page function
	result := &PageResult[*Entity]{
		Items:    entities,
		CursorId: nextCursorId,
		NextPage: func(ctx context.Context) (*PageResult[*Entity], error) {
			if nextCursorId < 0 {
				qlog.Trace("NextPage: No more results to fetch")

				// Clean up resources when we reach the end
				if builderRef != nil {
					_ = builderRef.Close()
					builderRef = nil
				}

				return &PageResult[*Entity]{
					Items:    []*Entity{},
					CursorId: -1,
					NextPage: nil,
				}, nil
			}
			qlog.Trace("NextPage: Fetching next page with cursorId: %d", nextCursorId)
			return sb.QueryWithPagination(ctx, entityType, query, pageSize, nextCursorId, opts...)
		},
		Cleanup: sb.Close,
	}

	qlog.Trace("QueryWithPagination: Returning result with %d items", len(result.Items))
	return result, nil
}

func (sb *SQLiteBuilder) RowToEntity(ctx context.Context, rows *sql.Rows, query *ParsedQuery) (*Entity, error) {
	// Get column names from the query
	columns, err := rows.Columns()
	if err != nil {
		qlog.Trace("RowToEntity: Failed to get column names: %v", err)
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}
	qlog.Trace("RowToEntity: Processing row with %d columns", len(columns))

	// Create a slice of interface{} to hold the values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Scan the row into the values slice
	if err := rows.Scan(valuePtrs...); err != nil {
		qlog.Trace("RowToEntity: Failed to scan row: %v", err)
		return nil, fmt.Errorf("failed to scan row: %v", err)
	}

	// First column should be the ID
	entityId := EntityId(values[0].(string))
	qlog.Trace("RowToEntity: Processing entity with ID: %s", entityId)

	// Check if entity is in cache first
	entity := sb.GetEntityFromCache(ctx, entityId)
	if entity == nil {
		qlog.Trace("RowToEntity: Entity not found in cache: %s", entityId)
		return nil, fmt.Errorf("entity not found in cache: %s", entityId)
	}

	qlog.Trace("RowToEntity: Successfully retrieved entity: %s", entityId)
	return entity, nil
}

func getSQLiteType(valueType ValueType) string {
	sqlType := ""
	switch valueType {
	case VTInt, VTChoice, VTBool:
		sqlType = "INTEGER"
	case VTFloat:
		sqlType = "REAL"
	case VTString, VTBinaryFile, VTEntityReference, VTEntityList:
		sqlType = "TEXT"
	case VTTimestamp:
		sqlType = "DATETIME"
	}
	qlog.Trace("getSQLiteType: Mapping value type %s to SQLite type %s", valueType, sqlType)
	return sqlType
}

func convertValueForSQLite(value *Value) interface{} {
	if value == nil {
		qlog.Trace("convertValueForSQLite: Received nil value")
		return nil
	}

	var result interface{}
	switch {
	case value.IsInt():
		result = value.GetInt()
	case value.IsFloat():
		result = value.GetFloat()
	case value.IsString():
		result = value.GetString()
	case value.IsBool():
		result = value.GetBool()
	case value.IsBinaryFile():
		result = value.GetBinaryFile()
	case value.IsEntityReference():
		result = value.AsString()
	case value.IsTimestamp():
		result = value.GetTimestamp()
	case value.IsChoice():
		result = value.GetChoice()
	case value.IsEntityList():
		result = value.AsString()
	default:
		result = nil
	}

	qlog.Trace("convertValueForSQLite: Converted value type: %T", result)
	return result
}
