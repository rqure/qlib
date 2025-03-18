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
	paginatedSQL, err := q.addPagination(q.stmt)
	if err != nil {
		qlog.Error("Failed to add pagination: %v", err)
		return false
	}

	rows, err := q.db.Query(paginatedSQL)
	if err != nil {
		qlog.Error("Failed to execute query: %v", err)
		return false
	}
	defer rows.Close()

	q.currentResult = q.processResults(ctx, rows)
	return true
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

	// Load entities
	if err := q.loadEntities(ctx); err != nil {
		q.db.Close()
		return fmt.Errorf("failed to load entities: %v", err)
	}

	// Get total count
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

	// Create custom functions via direct SQL instead of RegisterFunc
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
					WHEN id IS NOT NULL THEN unixepoch() -- placeholder for actual implementation
					ELSE NULL 
				END
			) STORED
		)
	`)
	return err
}

func (q *QueryV2) loadEntities(ctx context.Context) error {
	// Table already created in initDB()

	// Load all entities
	entityIds := q.store.FindEntities(ctx, q.entityType)
	multi := qbinding.NewMulti(q.store)

	for _, id := range entityIds {
		entity := qbinding.NewEntity(ctx, q.store, id)
		if err := q.addEntityToTable(ctx, entity); err != nil {
			return err
		}
	}

	multi.Commit(ctx)
	return nil
}

func (q *QueryV2) addEntityToTable(ctx context.Context, entity qdata.EntityBinding) error {
	// Get column names from the SQL statement
	columnMap := make(map[string]struct{})

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

	// Always include id column
	columnMap["id"] = struct{}{}

	// Begin transaction
	tx, err := q.db.Begin()
	if err != nil {
		return err
	}

	// Ensure columns exist for all fields
	for fieldName := range columnMap {
		_, err := tx.Exec(fmt.Sprintf("ALTER TABLE entities ADD COLUMN IF NOT EXISTS %s TEXT",
			q.sanitizeColumnName(fieldName)))
		if err != nil {
			tx.Rollback()
			return err
		}
	}

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
		tx.Rollback()
		return err
	}

	return tx.Commit()
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

func (q *QueryV2) addPagination(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// Add LIMIT and OFFSET
		offset := (q.currentPage - 1) * q.pageSize
		stmt.Limit = &sqlparser.Limit{
			Offset:   sqlparser.NewIntLiteral(fmt.Sprintf("%d", offset)),
			Rowcount: sqlparser.NewIntLiteral(fmt.Sprintf("%d", q.pageSize)),
		}

		return sqlparser.String(stmt), nil // Changed from stmt.String()
	}

	return "", fmt.Errorf("unsupported SQL statement type")
}

func (q *QueryV2) getTotalCount() (int, error) {
	switch stmt := q.stmt.(type) {
	case *sqlparser.Select:
		countStmt := &sqlparser.Select{
			SelectExprs: sqlparser.SelectExprs{
				&sqlparser.AliasedExpr{
					Expr: &sqlparser.FuncExpr{
						Name: sqlparser.NewIdentifierCI("COUNT"),
						Exprs: []sqlparser.Expr{
							sqlparser.NewIntLiteral("1"),
						},
					},
				},
			},
			From:  stmt.From,
			Where: stmt.Where,
		}

		sql := sqlparser.String(countStmt)
		var count int
		err := q.db.QueryRow(sql).Scan(&count)
		if err != nil {
			return 0, err
		}

		return count, nil
	}

	return 0, fmt.Errorf("unsupported SQL statement type")
}
