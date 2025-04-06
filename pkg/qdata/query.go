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

type QueryColumn struct {
	ColumnName string
	Alias      string
	Table      QueryTable
	IsSelected bool
}

func (me *QueryColumn) FinalName() string {
	if me.Alias != "" {
		return me.Alias
	}

	return me.ColumnName
}

func (me *QueryColumn) FieldType() FieldType {
	return FieldType(me.ColumnName)
}

type QueryTable struct {
	TableName string
	Alias     string
}

func (me *QueryTable) FinalName() string {
	if me.Alias != "" {
		return me.Alias
	}
	return me.TableName
}

func (me *QueryTable) EntityType() EntityType {
	return EntityType(me.TableName)
}

type ParsedQuery struct {
	Columns     []QueryColumn
	Tables      []QueryTable
	OriginalSQL string
	Where       *sqlparser.Where  // Add Where clause from parsed SQL
	OrderBy     sqlparser.OrderBy // Add OrderBy clause from parsed SQL
	GroupBy     sqlparser.GroupBy // Add GroupBy clause from parsed SQL
	Having      *sqlparser.Where  // Add Having clause from parsed SQL
}

type QueryRow map[string]*Value

type TypeHintMap map[string]ValueType
type TypeHintOpts func(TypeHintMap)

func TypeHint(columnName string, vt ValueType) TypeHintOpts {
	return func(m TypeHintMap) {
		m[columnName] = vt
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

	parsed := &ParsedQuery{
		Columns:     make([]QueryColumn, 0),
		Tables:      make([]QueryTable, 0),
		OriginalSQL: sql,
		Where:       selectStmt.Where,   // Store Where clause
		OrderBy:     selectStmt.OrderBy, // Store OrderBy clause
		GroupBy:     selectStmt.GroupBy, // Store GroupBy clause
		Having:      selectStmt.Having,  // Store Having clause
	}

	// Parse tables
	tableLookup := make(map[string]QueryTable) // map[alias]QueryTable

	// Track unique fields to avoid duplicates
	seenFields := make(map[string]bool)

	// Process tables and JOIN conditions in a single pass
	for _, tableExpr := range selectStmt.From {
		processTableExpr(tableExpr, tableLookup, &parsed.Tables, seenFields, &parsed.Columns)
	}

	// Parse fields from SELECT clause
	for _, expr := range selectStmt.SelectExprs {
		fields := extractFieldsFromExpr(expr, tableLookup, true)
		for _, field := range fields {
			if !seenFields[field.FinalName()] {
				parsed.Columns = append(parsed.Columns, field)
				seenFields[field.FinalName()] = true
				qlog.Trace("ParseQuery: Parsed SELECT field: %s, alias: %s", field.FieldType(), field.Alias)
			}
		}
	}

	// Extract fields from WHERE clause
	if selectStmt.Where != nil {
		fields := extractFieldsFromWhere(selectStmt.Where, tableLookup)
		for _, field := range fields {
			if !seenFields[field.FinalName()] {
				parsed.Columns = append(parsed.Columns, field)
				seenFields[field.FinalName()] = true
				qlog.Trace("ParseQuery: Parsed WHERE field: %s", field.FieldType())
			}
		}
	}

	// Extract fields from GROUP BY clause
	for _, groupBy := range selectStmt.GroupBy {
		fields := extractFieldsFromExpr(groupBy, tableLookup, false)
		for _, field := range fields {
			if !seenFields[field.FinalName()] {
				parsed.Columns = append(parsed.Columns, field)
				seenFields[field.FinalName()] = true
				qlog.Trace("ParseQuery: Parsed GROUP BY field: %s", field.FieldType())
			}
		}
	}

	// Extract fields from HAVING clause
	if selectStmt.Having != nil {
		fields := extractFieldsFromWhere(selectStmt.Having, tableLookup)
		for _, field := range fields {
			if !seenFields[field.FinalName()] {
				parsed.Columns = append(parsed.Columns, field)
				seenFields[field.FinalName()] = true
				qlog.Trace("ParseQuery: Parsed HAVING field: %s", field.FieldType())
			}
		}
	}

	// Extract fields from ORDER BY clause
	for _, orderBy := range selectStmt.OrderBy {
		fields := extractFieldsFromExpr(orderBy.Expr, tableLookup, false)
		for _, field := range fields {
			if !seenFields[field.FinalName()] {
				parsed.Columns = append(parsed.Columns, field)
				seenFields[field.FinalName()] = true
				qlog.Trace("ParseQuery: Parsed ORDER BY field: %s", field.FieldType())
			}
		}
	}

	qlog.Trace("ParseQuery: Successfully parsed query with %d fields", len(parsed.Columns))
	return parsed, nil
}

// Expanded helper function to process both tables and JOIN conditions
func processTableExpr(expr sqlparser.TableExpr, tableLookup map[string]QueryTable, tables *[]QueryTable,
	seenFields map[string]bool, columns *[]QueryColumn) {
	switch node := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := sqlparser.String(node.Expr)
		entityType := strings.Trim(tableName, "`")
		alias := node.As.String()
		if alias == "" {
			alias = entityType
		}
		queryTable := QueryTable{
			TableName: entityType,
			Alias:     alias,
		}
		tableLookup[alias] = queryTable
		*tables = append(*tables, queryTable)
		qlog.Trace("processTableExpr: Parsed table: %s with alias: %s", entityType, alias)

	case *sqlparser.JoinTableExpr:
		// Process both tables in the join
		processTableExpr(node.LeftExpr, tableLookup, tables, seenFields, columns)
		processTableExpr(node.RightExpr, tableLookup, tables, seenFields, columns)
		qlog.Trace("processTableExpr: Processed JOIN expression")

		// Extract fields from JOIN condition in the same pass
		if node.Condition.On != nil {
			fields := extractFieldsFromBoolExpr(node.Condition.On, tableLookup)
			for _, field := range fields {
				if !seenFields[field.FinalName()] {
					*columns = append(*columns, field)
					seenFields[field.FinalName()] = true
					qlog.Trace("processTableExpr: Parsed JOIN ON field: %s", field.FieldType())
				}
			}
		}

	case *sqlparser.ParenTableExpr:
		for _, tableExpr := range node.Exprs {
			processTableExpr(tableExpr, tableLookup, tables, seenFields, columns)
		}
		qlog.Trace("processTableExpr: Processed parenthesized table expression")
	}
}

func extractFieldsFromExpr(expr sqlparser.SQLNode, tableLookup map[string]QueryTable, isSelect bool) []QueryColumn {
	var fields []QueryColumn

	switch node := expr.(type) {
	case *sqlparser.AliasedExpr:
		field := extractField(node.Expr, tableLookup)
		if field != nil {
			if !node.As.IsEmpty() {
				field.Alias = node.As.String()
			}
			field.IsSelected = isSelect
			fields = append(fields, *field)
		}

	case *sqlparser.ColName:
		field := extractField(node, tableLookup)
		if field != nil {
			field.IsSelected = isSelect
			fields = append(fields, *field)
		}

	case *sqlparser.FuncExpr:
		// Extract fields from function arguments
		for _, arg := range node.Exprs {
			if ae, ok := arg.(*sqlparser.AliasedExpr); ok {
				fields = append(fields, extractFieldsFromExpr(ae.Expr, tableLookup, isSelect)...)
			}
		}

	case *sqlparser.Subquery:
		// Handle subquery expressions
		qlog.Trace("extractFieldsFromExpr: Processing subquery")

		// Parse the subquery
		subquery, err := sqlparser.Parse(sqlparser.String(node))
		if err != nil {
			qlog.Trace("extractFieldsFromExpr: Failed to parse subquery: %v", err)
			return fields
		}

		// Extract fields from the subquery's select list
		if subSelect, ok := subquery.(*sqlparser.Select); ok {
			for _, subExpr := range subSelect.SelectExprs {
				subFields := extractFieldsFromExpr(subExpr, tableLookup, isSelect)
				fields = append(fields, subFields...)
			}

			// Extract fields from the subquery's WHERE clause
			if subSelect.Where != nil {
				subWhereFields := extractFieldsFromWhere(subSelect.Where, tableLookup)
				fields = append(fields, subWhereFields...)
			}
		}

	case *sqlparser.BinaryExpr:
		// Extract fields from both sides of binary expressions
		leftFields := extractFieldsFromExpr(node.Left, tableLookup, isSelect)
		rightFields := extractFieldsFromExpr(node.Right, tableLookup, isSelect)
		fields = append(fields, leftFields...)
		fields = append(fields, rightFields...)
	}

	return fields
}

func extractField(expr sqlparser.Expr, tableLookup map[string]QueryTable) *QueryColumn {
	switch node := expr.(type) {
	case *sqlparser.ColName:
		qualifier := ""
		if node.Qualifier.Name.String() != "" {
			qualifier = node.Qualifier.Name.String()
		}
		columnName := node.Name.String()

		// If there's a table qualifier, use it to construct the field name
		if qualifier != "" {
			if queryTable, ok := tableLookup[qualifier]; ok {
				return &QueryColumn{
					ColumnName: columnName,
					Table:      queryTable,
					IsSelected: false, // Will be set by caller if needed
				}
			}
		} else {
			// If no qualifier and only one table, use the column name directly
			if len(tableLookup) == 1 {
				// Get the single table
				var queryTable QueryTable
				for _, qt := range tableLookup {
					queryTable = qt
					break
				}
				return &QueryColumn{
					ColumnName: columnName,
					Table:      queryTable,
					IsSelected: false, // Will be set by caller if needed
				}
			}
		}
	}

	return nil
}

func extractFieldsFromWhere(where *sqlparser.Where, tableLookup map[string]QueryTable) []QueryColumn {
	if where == nil {
		return nil
	}
	return extractFieldsFromBoolExpr(where.Expr, tableLookup)
}

func extractFieldsFromBoolExpr(expr sqlparser.Expr, tableLookup map[string]QueryTable) []QueryColumn {
	var fields []QueryColumn

	switch node := expr.(type) {
	case *sqlparser.ComparisonExpr:
		fields = append(fields, extractFieldsFromExpr(node.Left, tableLookup, false)...)
		fields = append(fields, extractFieldsFromExpr(node.Right, tableLookup, false)...)

	case *sqlparser.AndExpr:
		fields = append(fields, extractFieldsFromBoolExpr(node.Left, tableLookup)...)
		fields = append(fields, extractFieldsFromBoolExpr(node.Right, tableLookup)...)

	case *sqlparser.OrExpr:
		fields = append(fields, extractFieldsFromBoolExpr(node.Left, tableLookup)...)
		fields = append(fields, extractFieldsFromBoolExpr(node.Right, tableLookup)...)

	case *sqlparser.Subquery:
		// Handle subqueries in boolean expressions
		subqueryFields := extractFieldsFromExpr(node, tableLookup, false)
		fields = append(fields, subqueryFields...)

	case *sqlparser.BinaryExpr:
		// Extract fields from both sides of binary expressions
		leftFields := extractFieldsFromExpr(node.Left, tableLookup, false)
		rightFields := extractFieldsFromExpr(node.Right, tableLookup, false)
		fields = append(fields, leftFields...)
		fields = append(fields, rightFields...)

	case *sqlparser.ParenExpr:
		// Unwrap and process parenthesized expressions
		parenFields := extractFieldsFromBoolExpr(node.Expr, tableLookup)
		fields = append(fields, parenFields...)
	}

	return fields
}

type SQLiteBuilder struct {
	db        *sql.DB
	store     StoreInteractor
	typeHints TypeHintMap
	tableName string // Unique table name for this builder
	closed    bool   // Track if the builder has been closed
}

// Static counter for generating unique table names
var tableCounter int64 = 0

func NewSQLiteBuilder(store StoreInteractor) (*SQLiteBuilder, error) {
	qlog.Trace("NewSQLiteBuilder: Creating new SQLite builder")
	db, err := sql.Open("sqlite3", "")
	if err != nil {
		qlog.Trace("NewSQLiteBuilder: Failed to open in-memory SQLite database: %v", err)
		return nil, err
	}

	// Generate a unique table name using atomic counter
	uniqueID := atomic.AddInt64(&tableCounter, 1)
	tableName := fmt.Sprintf("entities_%d", uniqueID)
	qlog.Trace("NewSQLiteBuilder: Successfully created SQLite in-memory database with table name: %s", tableName)
	return &SQLiteBuilder{
		db:        db,
		store:     store,
		typeHints: make(TypeHintMap),
		tableName: tableName,
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

	me.closed = true
	qlog.Trace("SQLiteBuilder.Close: Resources cleaned up successfully")
	return nil
}

func (me *SQLiteBuilder) buildTable(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	qlog.Trace("Building SQLite table for entity type: %s with name: %s", entityType, me.tableName)

	// Drop the table if it exists - using ExecContext directly on the db connection
	_, err := me.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", me.tableName))
	if err != nil {
		qlog.Trace("buildTable: Failed to drop existing table: %v", err)
		return fmt.Errorf("failed to drop existing table: %v", err)
	}

	// Create table with all necessary columns
	columns := make([]string, 0)
	columns = append(columns, "[$CursorId] INTEGER PRIMARY KEY AUTOINCREMENT")
	columns = append(columns, "[$EntityId] TEXT")
	columns = append(columns, "[$EntityType] TEXT")
	for _, field := range query.Columns {
		var colType string

		finalName := field.FinalName()
		ft := field.FieldType()
		if ft.IsIndirection() {
			if vt, ok := me.typeHints[finalName]; ok {
				colType = getSQLiteType(vt)
			} else {
				colType = "TEXT"
			}
		} else {
			if vt, ok := me.typeHints[finalName]; ok {
				colType = getSQLiteType(vt)
			} else {
				schema := me.store.GetFieldSchema(ctx, entityType, ft)
				if schema != nil {
					colType = getSQLiteType(schema.ValueType)
					me.typeHints[finalName] = schema.ValueType
				}
			}
		}

		if colType != "" {
			columns = append(columns, fmt.Sprintf("[%s] %s", field.ColumnName, colType))

			// Add metadata columns if needed
			columns = append(columns, fmt.Sprintf("[%s$WriterId] TEXT", field.ColumnName))
			columns = append(columns, fmt.Sprintf("[%s$WriteTime] DATETIME", field.ColumnName))
		}
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", me.tableName, strings.Join(columns, ", "))
	qlog.Trace("Creating table with SQL: %s", createSQL)

	// Execute directly on the db connection
	_, err = me.db.ExecContext(ctx, createSQL)
	if err != nil {
		qlog.Trace("buildTable: Failed to create table: %v", err)
		return fmt.Errorf("failed to create table: %v", err)
	}

	qlog.Trace("buildTable: Successfully created table %s", me.tableName)
	return nil
}

func (me *SQLiteBuilder) populateTable(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	qlog.Trace("Populating table '%s'", entityType)

	// Begin a transaction for batch inserts
	tx, parentErr := me.db.BeginTx(ctx, nil)
	if parentErr != nil {
		return fmt.Errorf("failed to begin transaction: %v", parentErr)
	}
	defer func() {
		if parentErr != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf("INSERT OR IGNORE INTO %s ([$EntityId], [$EntityType]) VALUES (?, ?)", me.tableName))
	if err != nil {
		qlog.Warn("populateTable: Failed to prepare statement: %v", err)
		parentErr = fmt.Errorf("failed to prepare statement: %v", err)
		return parentErr
	}
	defer stmt.Close()

	entityIds := make([]EntityId, 0)
	me.store.FindEntities(entityType).ForEach(ctx, func(entityId EntityId) bool {
		entityIds = append(entityIds, entityId)
		qlog.Trace("Processing entity: %s", entityId)

		// Insert the entity ID first
		if _, err := stmt.ExecContext(ctx, entityId, entityType); err != nil {
			qlog.Warn("populateTable: Failed to insert entity %s: %v", entityId, err)
			parentErr = fmt.Errorf("failed to insert entity %s: %v", entityId, err)
			return false
		}
		return true
	})

	// Commit this transaction to ensure IDs are saved
	if err = tx.Commit(); err != nil {
		qlog.Warn("populateTable: Failed to commit entity IDs: %v", err)
		parentErr = fmt.Errorf("failed to commit entity IDs: %v", err)
		return parentErr
	}

	qlog.Trace("Batch insert complete, loading field data for %d entities", len(entityIds))
	// Now load field data in bulk for all entities in this batch
	if len(entityIds) > 0 {
		if err := me.loadQueryFieldsBulk(ctx, entityIds, query); err != nil {
			parentErr = fmt.Errorf("failed to load query fields: %v", err)
			return parentErr
		}
	}

	return parentErr
}

func (me *SQLiteBuilder) loadQueryFieldsBulk(ctx context.Context, entityIds []EntityId, query *ParsedQuery) error {
	qlog.Trace("loadQueryFieldsBulk: Loading fields for %d entities", len(entityIds))
	if len(entityIds) == 0 {
		qlog.Trace("loadQueryFieldsBulk: No entities to load")
		return nil
	}

	// Create a set of entities for which we need to get data
	allRequests := make([]*Request, 0)

	// Determine all the fields we need to fetch
	for _, entityId := range entityIds {
		entity := new(Entity).Init(entityId)
		for _, field := range query.Columns {
			ft := field.FieldType()
			qlog.Trace("loadQueryFieldsBulk: Adding read request for entity %s, field %s",
				entityId, ft)
			allRequests = append(allRequests, entity.Field(ft).AsReadRequest())
		}
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
	for _, req := range allRequests {
		entityId := req.EntityId

		if !req.Success {
			failCount++
			qlog.Trace("loadQueryFieldsBulk: Request failed for entity %s, field %s",
				entityId, req.FieldType.AsString())
			continue
		}
		successCount++

		fieldType := req.FieldType

		// Find the corresponding field in the query
		var queryField *QueryColumn
		for i := range query.Columns {
			if query.Columns[i].FieldType() == fieldType {
				queryField = &query.Columns[i]
				break
			}
		}

		if queryField == nil {
			qlog.Trace("loadQueryFieldsBulk: No matching query field for %s", fieldType.AsString())
			continue
		}

		// Direct field value
		qlog.Trace("loadQueryFieldsBulk: Updating value for entity %s, field %s",
			entityId, queryField.FieldType())
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`
                    UPDATE %s SET %s = ?, [%s$WriterId] = ?, [%s$WriteTime] = ? WHERE [$EntityId] = ?
                `, me.tableName, queryField.ColumnName, queryField.ColumnName, queryField.ColumnName),
			convertValueForSQLite(req.Value),
			req.WriterId.AsString(),
			req.WriteTime.AsTime(),
			entityId)
		if err != nil {
			qlog.Trace("loadQueryFieldsBulk: SQL error updating entity %s, field %s: %v",
				entityId, queryField.FieldType(), err)
			return fmt.Errorf("failed to update entity field: %v", err)
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

func (me *SQLiteBuilder) ExecuteQuery(ctx context.Context, query *ParsedQuery, limit int64, offset int64) (*sql.Rows, error) {
	qlog.Trace("ExecuteQuery: Building query with limit %d, offset %d", limit, offset)

	// Build the SELECT clause
	selectFields := make([]string, len(query.Columns))
	for i, field := range query.Columns {
		finalName := field.FinalName()
		selectFields[i] = fmt.Sprintf("%s as %s", field.ColumnName, finalName)
		qlog.Trace("ExecuteQuery: Added field select: %s", selectFields[i])
	}

	// Build the complete query
	sqlQuery := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectFields, ", "), me.tableName)

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

	if len(query.GroupBy) > 0 {
		groupBy := sqlparser.String(query.GroupBy)
		// Fix the GROUP BY clause - remove the "GROUP BY" keywords and any leading/trailing whitespace
		groupBy = strings.TrimSpace(groupBy)
		groupBy = strings.TrimPrefix(groupBy, "group by")
		groupBy = strings.TrimPrefix(groupBy, "GROUP BY")
		groupBy = strings.TrimSpace(groupBy)
		sqlQuery += " GROUP BY " + groupBy
		qlog.Trace("ExecuteQuery: Added GROUP BY clause: %s", groupBy)
	}

	if query.Having != nil {
		havingClause := sqlparser.String(query.Having)
		// Fix the HAVING clause - remove the "HAVING" keyword and any leading/trailing whitespace
		havingClause = strings.TrimSpace(havingClause)
		havingClause = strings.TrimPrefix(havingClause, "having")
		havingClause = strings.TrimPrefix(havingClause, "HAVING")
		havingClause = strings.TrimSpace(havingClause)
		sqlQuery += " HAVING " + havingClause
		qlog.Trace("ExecuteQuery: Added HAVING clause: %s", havingClause)
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
		sqlQuery += " ORDER BY [$CursorId]"
		qlog.Trace("ExecuteQuery: Using default ORDER BY id")
	}

	// Apply pagination
	sqlQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	qlog.Trace("ExecuteQuery: Final query: %s", sqlQuery)

	// Execute the query
	rows, err := me.db.QueryContext(ctx, sqlQuery)
	if err != nil {
		qlog.Error("ExecuteQuery: SQLite query failed: %v", err)
		return nil, err
	}

	qlog.Trace("ExecuteQuery: Query executed successfully")
	return rows, nil
}

func (me *SQLiteBuilder) QueryWithPagination(ctx context.Context, entityType EntityType, query *ParsedQuery, pageSize int64, cursorId int64, opts ...TypeHintOpts) (*PageResult[QueryRow], error) {
	qlog.Trace("QueryWithPagination: Starting for entity type %s, pageSize %d, cursorId %d",
		entityType, pageSize, cursorId)

	// Apply type hints
	for _, opt := range opts {
		opt(me.typeHints)
	}
	qlog.Trace("QueryWithPagination: Applied %d type hints", len(me.typeHints))

	// Create the SQLite table with the appropriate schema
	if err := me.buildTable(ctx, entityType, query); err != nil {
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
	err := me.populateTable(ctx, entityType, query)
	if err != nil {
		qlog.Trace("QueryWithPagination: Failed to populate table: %v", err)
		return nil, fmt.Errorf("failed to populate table: %v", err)
	}
	qlog.Trace("QueryWithPagination: Table populated.")

	// Execute query with exact pagination parameters
	rows, err := me.ExecuteQuery(ctx, query, pageSize, 0)
	if err != nil {
		qlog.Trace("QueryWithPagination: Failed to execute query: %v", err)
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()
	qlog.Trace("QueryWithPagination: Query executed, processing results")

	// Convert results to QueryRows
	var queryRows []QueryRow
	for rows.Next() {
		row, err := me.RowToQueryRow(rows, query)
		if err != nil {
			qlog.Error("QueryWithPagination: Failed to convert row: %v", err)
			continue
		}
		queryRows = append(queryRows, row)
	}

	qlog.Trace("QueryWithPagination: Processed %d rows", len(queryRows))

	// If we got fewer results than requested, we're at the end
	var nextCursorId int64 = -1
	if len(queryRows) == int(pageSize) {
		nextCursorId = cursorId + pageSize
	}

	// Create a reference to this builder to close it later
	builderRef := me

	// Create PageResult with next page function
	result := &PageResult[QueryRow]{
		Items:    queryRows,
		CursorId: nextCursorId,
		NextPage: func(ctx context.Context) (*PageResult[QueryRow], error) {
			if nextCursorId < 0 {
				qlog.Trace("NextPage: No more results to fetch")

				// Clean up resources when we reach the end
				if builderRef != nil {
					_ = builderRef.Close()
					builderRef = nil
				}

				return &PageResult[QueryRow]{
					Items:    []QueryRow{},
					CursorId: -1,
					NextPage: nil,
				}, nil
			}
			qlog.Trace("NextPage: Fetching next page with cursorId: %d", nextCursorId)
			return me.QueryWithPagination(ctx, entityType, query, pageSize, nextCursorId, opts...)
		},
		Cleanup: me.Close,
	}

	qlog.Trace("QueryWithPagination: Returning result with %d items", len(result.Items))
	return result, nil
}

func (me *SQLiteBuilder) RowToQueryRow(rows *sql.Rows, query *ParsedQuery) (QueryRow, error) {
	// Get column names from the rows
	columns, err := rows.Columns()
	if err != nil {
		qlog.Trace("RowToQueryRow: Failed to get column names: %v", err)
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	// Create slices to hold the values
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Scan the row into the values slice
	if err := rows.Scan(valuePtrs...); err != nil {
		qlog.Trace("RowToQueryRow: Failed to scan row: %v", err)
		return nil, fmt.Errorf("failed to scan row: %v", err)
	}

	// Create a map to hold the column values
	queryRow := make(QueryRow)

	// Use the original field names/aliases from the query
	for i, value := range values {
		if value == nil {
			continue
		}

		// Find the corresponding query field for this column
		field := query.Columns[i]
		key := field.FinalName()

		vt, ok := me.typeHints[key]
		if !ok {
			qlog.Trace("RowToQueryRow: No type hint for column [%s]", key)
			continue
		}

		queryRow[key] = vt.NewValue(value)
	}

	return queryRow, nil
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
