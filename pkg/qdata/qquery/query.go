package qquery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qlog"
	"vitess.io/vitess/go/vt/sqlparser"
)

const DefaultPageSize = 1000

type Query struct {
	entityManager qdata.EntityManager

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

func New(entityManager qdata.EntityManager) qdata.Query {
	return &Query{
		entityManager: entityManager,
		pageSize:      DefaultPageSize, // Default page size
		entityCache:   make(map[string]qdata.EntityBinding),
	}
}

func (me *Query) Prepare(sql string) qdata.Query {
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		qlog.Error("Failed to create SQL parser: %v", err)
		return me
	}

	me.stmt, err = parser.Parse(sql)
	if err != nil {
		qlog.Error("Failed to parse SQL: %v", err)
		return me
	}

	// Extract entity type from the FROM clause
	switch stmt := me.stmt.(type) {
	case *sqlparser.Select:
		if len(stmt.From) > 0 {
			tableExpr := stmt.From[0]
			if aliasedTableExpr, ok := tableExpr.(*sqlparser.AliasedTableExpr); ok {
				if tableName, ok := aliasedTableExpr.Expr.(sqlparser.TableName); ok {
					me.entityType = tableName.Name.String()
				}
			}
		}
	}

	// Process the AST to handle field indirection
	processedSQL, err := me.processAST(me.stmt)
	if err != nil {
		qlog.Error("Failed to process SQL AST: %v", err)
		return me
	}

	me.sqlQuery = processedSQL
	return me
}

func (me *Query) WithPageSize(size int) qdata.Query {
	me.pageSize = size
	return me
}

func (me *Query) Next(ctx context.Context) bool {
	if !me.initialized {
		if err := me.initialize(ctx); err != nil {
			qlog.Error("Failed to initialize query: %v", err)
			return false
		}
		me.initialized = true

		// Execute the full query once with row numbers
		if err := me.executeFullQuery(); err != nil {
			qlog.Error("Failed to execute full query: %v", err)
			return false
		}
	}

	// Check if we've reached the end
	if me.currentPage >= me.pageCount {
		return false
	}

	// Increment page and fetch results
	me.currentPage++
	offset := (me.currentPage - 1) * me.pageSize

	// Query with pagination using row numbers from the window function
	rows, err := me.db.Query(fmt.Sprintf(`
        WITH numbered_results AS (
            SELECT *, ROW_NUMBER() OVER () as row_num 
            FROM (%s)
        )
        SELECT id FROM numbered_results 
        WHERE row_num > ? AND row_num <= ?
    `, me.sqlQuery), offset, offset+me.pageSize)

	if err != nil {
		qlog.Error("Failed to execute paginated query: %v", err)
		return false
	}
	defer rows.Close()

	me.currentResult = me.processResultsFromFinal(rows)
	return len(me.currentResult) > 0
}

func (me *Query) GetCurrentPage() []qdata.EntityBinding {
	return me.currentResult
}

func (me *Query) GetTotalCount() int {
	return me.totalCount
}

func (me *Query) GetPageCount() int {
	return me.pageCount
}

func (me *Query) GetCurrentPageNum() int {
	return me.currentPage
}

func (me *Query) initialize(ctx context.Context) error {
	if me.stmt == nil {
		return fmt.Errorf("no SQL statement provided")
	}

	// Initialize database
	if err := me.initDB(); err != nil {
		return fmt.Errorf("failed to initialize SQLite: %v", err)
	}

	// Process entities and execute query
	if err := me.processEntitiesAndExecuteQuery(ctx); err != nil {
		me.db.Close()
		return fmt.Errorf("failed to process entities: %v", err)
	}

	// Get total count from results table
	count, err := me.getTotalCount()
	if err != nil {
		me.db.Close()
		return fmt.Errorf("failed to get total count: %v", err)
	}

	me.totalCount = count
	me.pageCount = (count + me.pageSize - 1) / me.pageSize
	me.currentPage = 0

	return nil
}

func (me *Query) initDB() error {
	var err error
	me.db, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		return err
	}

	// Create single entities table with just the ID
	_, err = me.db.Exec(`
        CREATE TABLE entities (
            id TEXT PRIMARY KEY
        )
    `)

	return err
}

func (me *Query) processEntitiesAndExecuteQuery(ctx context.Context) error {
	// Extract column names needed for this query
	columnMap := me.extractRequiredColumns()

	// Ensure columns exist in entities table
	if err := me.ensureColumns(ctx, columnMap); err != nil {
		return err
	}

	// Process entities in batches
	batchSize := DefaultPageSize
	currentOffset := 0

	for {
		result := me.entityManager.FindEntitiesPaginated(ctx, me.entityType, currentOffset/batchSize, batchSize)
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

		if err := me.processBatch(ctx, entityIds, columnMap); err != nil {
			return err
		}

		currentOffset += len(entityIds)
	}

	return nil
}

func (me *Query) extractRequiredColumns() map[string]struct{} {
	columnMap := make(map[string]struct{})

	// Always include id column
	columnMap["id"] = struct{}{}

	switch stmt := me.stmt.(type) {
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
			me.extractColumnsFromExpr(stmt.Where.Expr, columnMap)
		}

		// Extract columns from ORDER BY
		for _, orderBy := range stmt.OrderBy {
			me.extractColumnsFromExpr(orderBy.Expr, columnMap)
		}
	}

	return columnMap
}

func (me *Query) processBatch(ctx context.Context, entityIds []string, columnMap map[string]struct{}) error {
	multi := qbinding.NewMulti(me.store)

	tx, err := me.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Prepare columns for INSERT statement - each field has value, writer, write_time
	var columns []string
	for fieldName := range columnMap {
		sanitized := me.sanitizeColumnName(fieldName)
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
		entity := qbinding.NewEntity(ctx, me.store, entityId)
		me.entityCache[entityId] = entity

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
				values[valueIdx] = me.getFieldValue(field)
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

func (me *Query) getFieldValue(field qdata.FieldBinding) interface{} {
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

func (me *Query) extractColumnsFromExpr(expr sqlparser.Expr, columnMap map[string]struct{}) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		columnMap[expr.Name.String()] = struct{}{}
	case *sqlparser.ComparisonExpr:
		me.extractColumnsFromExpr(expr.Left, columnMap)
		me.extractColumnsFromExpr(expr.Right, columnMap)
	case *sqlparser.AndExpr:
		me.extractColumnsFromExpr(expr.Left, columnMap)
		me.extractColumnsFromExpr(expr.Right, columnMap)
	case *sqlparser.OrExpr:
		me.extractColumnsFromExpr(expr.Left, columnMap)
		me.extractColumnsFromExpr(expr.Right, columnMap)
	}
}

func (me *Query) processAST(stmt sqlparser.Statement) (string, error) {
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		// Process SELECT expressions for field indirection
		for _, expr := range stmt.SelectExprs {
			if aliasedExpr, ok := expr.(*sqlparser.AliasedExpr); ok {
				processed, err := me.processExpression(aliasedExpr.Expr)
				if err != nil {
					return "", err
				}
				aliasedExpr.Expr = processed
			}
		}

		// Process WHERE clause
		if stmt.Where != nil {
			processed, err := me.processExpression(stmt.Where.Expr)
			if err != nil {
				return "", err
			}
			stmt.Where.Expr = processed
		}

		// Process ORDER BY
		for _, orderBy := range stmt.OrderBy {
			processed, err := me.processExpression(orderBy.Expr)
			if err != nil {
				return "", err
			}
			orderBy.Expr = processed
		}

		return sqlparser.String(stmt), nil
	}

	return "", fmt.Errorf("unsupported SQL statement type")
}

func (me *Query) processExpression(expr sqlparser.Expr) (sqlparser.Expr, error) {
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		// Handle field indirection and case sensitivity in column names
		name := expr.Name.String()
		if strings.Contains(name, "->") {
			// Create a new ColName with the sanitized identifier
			sanitized := me.sanitizeColumnName(name)
			actual := me.findActualColumnName(sanitized)
			return &sqlparser.ColName{
				Name: sqlparser.NewIdentifierCI(actual),
			}, nil
		}
		// Handle case sensitivity for regular column names
		actual := me.findActualColumnName(name)
		return &sqlparser.ColName{
			Name: sqlparser.NewIdentifierCI(actual),
		}, nil

	case *sqlparser.ComparisonExpr:
		// Process both sides of the comparison
		left, err := me.processExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := me.processExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		expr.Left = left
		expr.Right = right
		return expr, nil

	case *sqlparser.AndExpr:
		left, err := me.processExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := me.processExpression(expr.Right)
		if err != nil {
			return nil, err
		}
		expr.Left = left
		expr.Right = right
		return expr, nil

	case *sqlparser.OrExpr:
		left, err := me.processExpression(expr.Left)
		if err != nil {
			return nil, err
		}
		right, err := me.processExpression(expr.Right)
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
func (me *Query) findActualColumnName(name string) string {
	// Query the table schema to get the actual column names
	rows, err := me.db.Query("SELECT name FROM pragma_table_info('entities')")
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

func (me *Query) getTotalCount() (int, error) {
	var count int
	err := me.db.QueryRow("SELECT COUNT(*) FROM query_results").Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// New method to process results from the final results table
func (me *Query) processResultsFromFinal(rows *sql.Rows) []qdata.EntityBinding {
	var results []qdata.EntityBinding

	// We only need to scan the ID since that's all we store now
	var id string
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			continue
		}

		// Get the cached entity
		if entity, ok := me.entityCache[id]; ok {
			results = append(results, entity)
		}
	}

	return results
}

func (me *Query) executeFullQuery() error {
	// Count total results
	var count int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", me.sqlQuery)
	if err := me.db.QueryRow(countQuery).Scan(&count); err != nil {
		return err
	}

	me.totalCount = count
	me.pageCount = (count + me.pageSize - 1) / me.pageSize
	return nil
}

func (me *Query) sanitizeColumnName(name string) string {
	// Convert field indirection syntax to valid SQLite column name
	// Example: "NextStation->Name" becomes "NextStation_Name"
	return strings.ReplaceAll(name, "->", "_")
}

func (me *Query) ensureColumns(ctx context.Context, columnMap map[string]struct{}) error {
	tx, err := me.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	entitySchema := me.store.GetEntitySchema(ctx, me.entityType)
	if entitySchema == nil {
		return fmt.Errorf("entity type %s not found", me.entityType)
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

		sqlType = me.getSQLiteType(fieldSchema.GetFieldType())
		columnName := me.sanitizeColumnName(fieldName)

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

func (me *Query) getSQLiteType(fieldType string) string {
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
