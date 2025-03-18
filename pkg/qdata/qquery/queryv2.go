package qquery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
	"github.com/rqure/qlib/pkg/qlog"
	"vitess.io/vitess/go/vt/sqlparser"
)

type QueryV2 struct {
	store      qdata.Store
	entityType string
	sqlQuery   string
	stmt       sqlparser.Statement
	db         *sql.DB
	pageSize   int

	// Iterator state
	currentPage   int
	currentResult []qdata.EntityBinding
	totalCount    int
	pageCount     int
	initialized   bool

	// Processing state
	entityIdsMap map[string]bool // To track processed entity IDs
}

func NewV2(s qdata.Store) qdata.QueryV2 {
	return &QueryV2{
		store:    s,
		pageSize: 50, // Default page size
	}
}

func (q *QueryV2) WithSQL(sql string) qdata.QueryV2 {
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		qlog.Error("Failed to create SQL parser: %v", err)
		return q
	}

	q.stmt, err = parser.Parse(sql)
	if err != nil {
		qlog.Error("Failed to parse SQL: %v", err)
		return q
	}

	// Extract entity type from the FROM clause
	switch stmt := q.stmt.(type) {
	case *sqlparser.Select:
		if len(stmt.From) > 0 {
			tableExpr := stmt.From[0]
			if aliasedTableExpr, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
				if tableName, ok := aliasedTableExpr.Expr.(sqlparser.TableName); ok {
					q.entityType = tableName.Name.String()
				}
			}
		}
	}

	// Process the AST to handle field indirection
	processedSQL, err := q.processAST(q.stmt)
	if err != nil {
		qlog.Error("Failed to process SQL AST: %v", err)
		return q
	}

	q.sqlQuery = processedSQL
	return q
}

func (q *QueryV2) PageSize(size int) qdata.QueryV2 {
	q.pageSize = size
	return q
}

func (q *QueryV2) Next(ctx context.Context) bool {
	// Initialize if this is the first call
	if !q.initialized {
		if err := q.initialize(ctx); err != nil {
			qlog.Error("Failed to initialize query: %v", err)
			return false
		}
		q.initialized = true
	}

	// Check if we've reached the end
	if q.currentPage >= q.pageCount {
		return false
	}

	// Increment page and fetch results
	q.currentPage++
	offset := (q.currentPage - 1) * q.pageSize

	// Query directly from the results table with pagination, no join needed
	rows, err := q.db.Query(
		`SELECT * FROM query_results ORDER BY row_num LIMIT ? OFFSET ?`,
		q.pageSize, offset,
	)

	if err != nil {
		qlog.Error("Failed to execute paginated query: %v", err)
		return false
	}
	defer rows.Close()

	q.currentResult = q.processResultsFromFinal(ctx, rows)
	return len(q.currentResult) > 0
}

func (q *QueryV2) GetCurrentPage() []qdata.EntityBinding {
	return q.currentResult
}

func (q *QueryV2) GetTotalCount() int {
	return q.totalCount
}

func (q *QueryV2) GetPageCount() int {
	return q.pageCount
}

func (q *QueryV2) GetCurrentPageNum() int {
	return q.currentPage
}

func (q *QueryV2) initialize(ctx context.Context) error {
	if q.stmt == nil {
		return fmt.Errorf("no SQL statement provided")
	}

	// Initialize database
	if err := q.initDB(); err != nil {
		return fmt.Errorf("failed to initialize SQLite: %v", err)
	}

	q.entityIdsMap = make(map[string]bool)

	// Process entities and execute query
	if err := q.processEntitiesAndExecuteQuery(ctx); err != nil {
		q.db.Close()
		return fmt.Errorf("failed to process entities: %v", err)
	}

	// Get total count from results table
	count, err := q.getTotalCount()
	if err != nil {
		q.db.Close()
		return fmt.Errorf("failed to get total count: %v", err)
	}

	q.totalCount = count
	q.pageCount = (count + q.pageSize - 1) / q.pageSize
	q.currentPage = 0

	return nil
}

func (q *QueryV2) initDB() error {
	var err error
	q.db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}

	// Create entities table for processing batches
	_, err = q.db.Exec(`
		CREATE TABLE entities (
			id TEXT PRIMARY KEY,
			writer TEXT GENERATED ALWAYS AS (
				CASE 
					WHEN id IS NOT NULL THEN ''
					ELSE NULL 
				END
			) STORED,
			write_time INTEGER GENERATED ALWAYS AS (
				CASE 
					WHEN id IS NOT NULL THEN unixepoch()
					ELSE NULL 
				END
			) STORED
		)
	`)

	if err != nil {
		return err
	}

	// Create results table with just ID and row_num initially
	// We'll add columns as needed based on the query
	_, err = q.db.Exec(`CREATE TABLE query_results (
		id TEXT PRIMARY KEY,
		row_num INTEGER
	)`)

	return err
}

func (q *QueryV2) processEntitiesAndExecuteQuery(ctx context.Context) error {
	// Extract column names needed for this query
	columnMap := q.extractRequiredColumns()

	// Ensure columns exist for entities table
	if err := q.ensureColumns("entities", columnMap); err != nil {
		return err
	}

	// Also ensure the same columns exist in results table
	if err := q.ensureColumns("query_results", columnMap); err != nil {
		return err
	}

	// Process entities in batches
	batchSize := 100
	currentOffset := 0

	for {
		// Get a batch of entity IDs
		result := q.store.FindEntitiesPaginated(ctx, q.entityType, currentOffset/batchSize, batchSize)
		if result == nil {
			return fmt.Errorf("failed to get paginated entities")
		}

		var entityIds []string
		hasEntities := false

		// Collect entity IDs for this batch
		for result.Next(ctx) {
			entityId := result.Value()
			if entityId != "" && !q.entityIdsMap[entityId] {
				entityIds = append(entityIds, entityId)
				q.entityIdsMap[entityId] = true
				hasEntities = true
			}

			if err := result.Error(); err != nil {
				return fmt.Errorf("error during entity pagination: %v", err)
			}
		}

		// If no more entities, we're done
		if !hasEntities {
			break
		}

		// Process this batch of entities
		if err := q.processBatch(ctx, entityIds, columnMap); err != nil {
			return err
		}

		// Execute the query on current data and append to results
		if err := q.appendQueryResults(columnMap); err != nil {
			return err
		}

		// Clear entities table for next batch to save memory
		if _, err := q.db.Exec("DELETE FROM entities"); err != nil {
			return fmt.Errorf("failed to clear entities table: %v", err)
		}

		// Move to next batch
		currentOffset += len(entityIds)
	}

	return nil
}

func (q *QueryV2) extractRequiredColumns() map[string]struct{} {
	columnMap := make(map[string]struct{})

	// Always include id column
	columnMap["id"] = struct{}{}

	switch stmt := q.stmt.(type) {
	case *sqlparser.Select:
		// Extract columns from SELECT expressions
		for _, expr := range stmt.SelectExprs {
			if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
				if colName, ok := aliasedExpr.Expr.(*sqlparser.ColName); ok {
					columnMap[colName.Name.String()] = struct{}{}
				}
			}
		}

		// Extract columns from WHERE clause
		if stmt.Where != nil {
			q.extractColumnsFromExpr(stmt.Where.Expr, columnMap)
		}

		// Extract columns from ORDER BY
		for _, orderBy := range stmt.OrderBy {
			q.extractColumnsFromExpr(orderBy.Expr, columnMap)
		}
	}

	return columnMap
}

func (q *QueryV2) ensureColumns(tableName string, columnMap map[string]struct{}) error {
	// Ensure columns exist for all fields in the specified table
	for fieldName := range columnMap {
		// Skip id as it's already in the table schema
		if fieldName == "id" {
			continue
		}

		_, err := q.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT",
			tableName, q.sanitizeColumnName(fieldName)))
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *QueryV2) processBatch(ctx context.Context, entityIds []string, columnMap map[string]struct{}) error {
	multi := qbinding.NewMulti(q.store)
	defer multi.Commit(ctx)

	for _, entityId := range entityIds {
		entity := qbinding.NewEntity(ctx, q.store, entityId)
		if err := q.addEntityToTable(ctx, entity, columnMap); err != nil {
			return err
		}
	}

	return nil
}

func (q *QueryV2) addEntityToTable(ctx context.Context, entity qdata.EntityBinding, columnMap map[string]struct{}) error {
	// Begin transaction
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Will be committed if everything goes well

	// Insert entity data
	cols := []string{"id"}
	vals := []interface{}{entity.GetId()}
	placeholders := []string{"?"}

	// Add field values
	for fieldName := range columnMap {
		if fieldName == "id" {
			continue
		}

		// Use GetField and ReadValue to handle indirect fields
		field := entity.GetField(fieldName)
		if field != nil {
			field.ReadValue(ctx) // Ensure the field value is loaded
			if field.GetValue() != nil {
				cols = append(cols, q.sanitizeColumnName(fieldName))
				vals = append(vals, q.getFieldValue(field))
				placeholders = append(placeholders, "?")
			}
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO entities (%s) VALUES (%s)",
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	if _, err := tx.Exec(query, vals...); err != nil {
		return err
	}

	return tx.Commit()
}

func (q *QueryV2) appendQueryResults(columnMap map[string]struct{}) error {
	// Create a processed version of the SQL query that inserts all needed columns into results table
	resultSQL, err := q.createResultInsertSQL(columnMap)
	if err != nil {
		return fmt.Errorf("failed to create results SQL: %v", err)
	}

	// Execute the query and store results with all necessary columns
	_, err = q.db.Exec(resultSQL)
	if err != nil {
		return fmt.Errorf("failed to execute result query: %v", err)
	}

	return nil
}

func (q *QueryV2) createResultInsertSQL(columnMap map[string]struct{}) (string, error) {
	switch stmt := q.stmt.(type) {
	case *sqlparser.Select:
		// Build column list for INSERT statement
		columns := []string{"id", "row_num"}
		selectColumns := []string{"id", "(SELECT COALESCE(MAX(row_num), 0) + 1 FROM query_results)"}

		// Add all required columns
		for fieldName := range columnMap {
			if fieldName == "id" {
				continue
			}
			sanitizedName := q.sanitizeColumnName(fieldName)
			columns = append(columns, sanitizedName)
			selectColumns = append(selectColumns, sanitizedName)
		}

		// Process the AST to handle field indirection
		processedSQL, err := q.processAST(stmt)
		if err != nil {
			return "", err
		}

		// Build insert statement that includes all columns
		insertSQL := fmt.Sprintf(
			"INSERT OR IGNORE INTO query_results (%s) SELECT %s FROM (%s)",
			strings.Join(columns, ", "),
			strings.Join(selectColumns, ", "),
			processedSQL,
		)

		return insertSQL, nil
	}

	return "", fmt.Errorf("unsupported SQL statement type")
}

func (q *QueryV2) extractColumnsFromExpr(expr sqlparser.Expr, columnMap map[string]struct{}) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		columnMap[expr.Name.String()] = struct{}{}
	case *sqlparser.ComparisonExpr:
		q.extractColumnsFromExpr(expr.Left, columnMap)
		q.extractColumnsFromExpr(expr.Right, columnMap)
	case *sqlparser.AndExpr:
		q.extractColumnsFromExpr(expr.Left, columnMap)
		q.extractColumnsFromExpr(expr.Right, columnMap)
	case *sqlparser.OrExpr:
		q.extractColumnsFromExpr(expr.Left, columnMap)
		q.extractColumnsFromExpr(expr.Right, columnMap)
	}
}

func (q *QueryV2) processAST(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// Process SELECT expressions for field indirection
		for _, expr := range stmt.SelectExprs {
			if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
				processed, err := q.processExpression(aliasedExpr.Expr)
				if err != nil {
					return "", err
				}
				aliasedExpr.Expr = processed
			}
		}

		// Process WHERE clause
		if stmt.Where != nil {
			processed, err := q.processExpression(stmt.Where.Expr)
			if err != nil {
				return "", err
			}
			stmt.Where.Expr = processed
		}

		// Process ORDER BY
		for _, orderBy := range stmt.OrderBy {
			processed, err := q.processExpression(orderBy.Expr)
			if err != nil {
				return "", err
			}
			orderBy.Expr = processed
		}

		return sqlparser.String(stmt), nil
	}

	return "", fmt.Errorf("unsupported SQL statement type")
}

func (q *QueryV2) processExpression(expr sqlparser.Expr) (sqlparser.Expr, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		// Handle field indirection and case sensitivity in column names
		name := expr.Name.String()
		if strings.Contains(name, "->") {
			// Create a new ColName with the sanitized identifier
			sanitized := q.sanitizeColumnName(name)
			actual := q.findActualColumnName(sanitized)
			return &sqlparser.ColName{
				Name: sqlparser.NewIdentifierCI(actual),
			}, nil
		}
		// Handle case sensitivity for regular column names
		actual := q.findActualColumnName(name)
		return &sqlparser.ColName{
			Name: sqlparser.NewIdentifierCI(actual),
		}, nil

	case *sqlparser.ComparisonExpr:
		// Process both sides of the comparison
		left, err := q.processExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.processExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		expr.Left = left
		expr.Right = right
		return expr, nil

	case *sqlparser.AndExpr:
		left, err := q.processExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.processExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		expr.Left = left
		expr.Right = right
		return expr, nil

	case *sqlparser.OrExpr:
		left, err := q.processExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := q.processExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		expr.Left = left
		expr.Right = right
		return expr, nil
	}

	return expr, nil
}

// New helper method to find the actual case-sensitive column name
func (q *QueryV2) findActualColumnName(name string) string {
	// Query the table schema to get the actual column names
	rows, err := q.db.Query("SELECT name FROM pragma_table_info('entities')")
	if err != nil {
		return name // Return original name if we can't query schema
	}
	defer rows.Close()

	var columnName string
	for rows.Next() {
		err := rows.Scan(&columnName)
		if err != nil {
			continue
		}
		if strings.EqualFold(columnName, name) {
			return columnName // Return the actual case-sensitive name
		}
	}
	return name // Return original name if no match found
}

func (q *QueryV2) getTotalCount() (int, error) {
	var count int
	err := q.db.QueryRow("SELECT COUNT(*) FROM query_results").Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// New method to process results from the final results table
func (q *QueryV2) processResultsFromFinal(ctx context.Context, rows *sql.Rows) []qdata.EntityBinding {
	columns, err := rows.Columns()
	if err != nil {
		return nil
	}

	var results []qdata.EntityBinding
	multi := qbinding.NewMulti(q.store)
	idIndex := -1

	// Find the index of the ID column
	for i, col := range columns {
		if col == "id" {
			idIndex = i
			break
		}
	}

	if idIndex == -1 {
		return nil // No ID column found
	}

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

		// Get entity ID
		idValue := values[idIndex]
		if id, ok := idValue.(string); ok && id != "" {
			entity := qbinding.NewEntity(ctx, q.store, id)

			// Add entity to results
			results = append(results, entity)
		}
	}

	multi.Commit(ctx)
	return results
}
