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
	entityCache map[string]qdata.EntityBinding // Cache for entity field values
}

func NewV2(s qdata.Store) qdata.QueryV2 {
	return &QueryV2{
		store:       s,
		pageSize:    50, // Default page size
		entityCache: make(map[string]qdata.EntityBinding),
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
	if !q.initialized {
		if err := q.initialize(ctx); err != nil {
			qlog.Error("Failed to initialize query: %v", err)
			return false
		}
		q.initialized = true

		// Execute the full query once with row numbers
		if err := q.executeFullQuery(); err != nil {
			qlog.Error("Failed to execute full query: %v", err)
			return false
		}
	}

	// Check if we've reached the end
	if q.currentPage >= q.pageCount {
		return false
	}

	// Increment page and fetch results
	q.currentPage++
	offset := (q.currentPage - 1) * q.pageSize

	// Query with pagination using row numbers from the window function
	rows, err := q.db.Query(fmt.Sprintf(`
        WITH numbered_results AS (
            SELECT *, ROW_NUMBER() OVER () as row_num 
            FROM (%s)
        )
        SELECT id FROM numbered_results 
        WHERE row_num > ? AND row_num <= ?
    `, q.sqlQuery), offset, offset+q.pageSize)

	if err != nil {
		qlog.Error("Failed to execute paginated query: %v", err)
		return false
	}
	defer rows.Close()

	q.currentResult = q.processResultsFromFinal(rows)
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

	// Create single entities table with just the ID
	_, err = q.db.Exec(`
        CREATE TABLE entities (
            id TEXT PRIMARY KEY
        )
    `)

	return err
}

func (q *QueryV2) processEntitiesAndExecuteQuery(ctx context.Context) error {
	// Extract column names needed for this query
	columnMap := q.extractRequiredColumns()

	// Ensure columns exist in entities table
	if err := q.ensureColumns(ctx, columnMap); err != nil {
		return err
	}

	// Process entities in batches
	batchSize := 100
	currentOffset := 0

	for {
		result := q.store.FindEntitiesPaginated(ctx, q.entityType, currentOffset/batchSize, batchSize)
		if result == nil {
			return fmt.Errorf("failed to get paginated entities")
		}

		var entityIds []string
		hasEntities := false

		for result.Next(ctx) {
			entityId := result.Value()
			if entityId != "" {
				entityIds = append(entityIds, entityId)
				hasEntities = true
			}
		}

		if !hasEntities {
			break
		}

		if err := q.processBatch(ctx, entityIds, columnMap); err != nil {
			return err
		}

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

func (q *QueryV2) processBatch(ctx context.Context, entityIds []string, columnMap map[string]struct{}) error {
	multi := qbinding.NewMulti(q.store)

	tx, err := q.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare columns for INSERT statement - each field has value, writer, write_time
	var columns []string
	for fieldName := range columnMap {
		sanitized := q.sanitizeColumnName(fieldName)
		if sanitized == "id" {
			columns = append(columns, "id")
			continue
		}
		columns = append(columns,
			sanitized,
			sanitized+"_writer",
			sanitized+"_write_time",
		)
	}

	// Create INSERT statement with placeholders
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf(
		"INSERT INTO entities (%s) VALUES (%s)",
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, entityId := range entityIds {
		entity := qbinding.NewEntity(ctx, q.store, entityId)
		q.entityCache[entityId] = entity

		// Prepare values for all columns including writer and write_time
		values := make([]interface{}, len(columns))
		valueIdx := 0

		// Handle ID first
		for i, col := range columns {
			if col == "id" {
				values[i] = entityId
				valueIdx = i + 1
				break
			}
		}

		// Handle remaining fields
		for fieldName := range columnMap {
			if fieldName == "id" {
				continue
			}

			field := entity.GetField(fieldName)
			if field != nil {
				field.ReadValue(ctx)
				values[valueIdx] = q.getFieldValue(field)
				values[valueIdx+1] = field.GetWriter()
				values[valueIdx+2] = field.GetWriteTime().Unix()
			} else {
				values[valueIdx] = nil
				values[valueIdx+1] = nil
				values[valueIdx+2] = nil
			}
			valueIdx += 3
		}

		if _, err := stmt.Exec(values...); err != nil {
			return err
		}
	}

	multi.Commit(ctx)
	return tx.Commit()
}

func (q *QueryV2) getFieldValue(field qdata.FieldBinding) interface{} {
	if field == nil || field.GetValue() == nil {
		return nil
	}

	value := field.GetValue()
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
	case value.IsChoice():
		return value.GetChoice().Index()
	case value.IsEntityList():
		// Convert entity list to JSON array of IDs
		entities := value.GetEntityList().GetEntities()
		return strings.Join(entities, ",")
	default:
		return nil
	}
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
func (q *QueryV2) processResultsFromFinal(rows *sql.Rows) []qdata.EntityBinding {
	var results []qdata.EntityBinding

	// We only need to scan the ID since that's all we store now
	var id string
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			continue
		}

		// Get the cached entity
		if entity, ok := q.entityCache[id]; ok {
			results = append(results, entity)
		}
	}

	return results
}

func (q *QueryV2) executeFullQuery() error {
	// Count total results
	var count int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", q.sqlQuery)
	if err := q.db.QueryRow(countQuery).Scan(&count); err != nil {
		return err
	}

	q.totalCount = count
	q.pageCount = (count + q.pageSize - 1) / q.pageSize
	return nil
}
