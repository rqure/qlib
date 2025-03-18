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
}

func NewV2(s qdata.Store) qdata.Query {
	return &QueryV2{
		store: s,
	}
}

func (q *QueryV2) ForType(t string) qdata.Query {
	q.entityType = t
	return q
}

func (q *QueryV2) From(t string) qdata.Query {
	return q.ForType(t)
}

func (q *QueryV2) Select(fields ...string) qdata.Query {
	// Convert fields to SQL SELECT statement
	q.sqlQuery = fmt.Sprintf("SELECT %s FROM %s", strings.Join(fields, ", "), q.entityType)
	return q
}

func (q *QueryV2) Where(fieldName string) qdata.FieldQuery {
	// Not used in V2, but required by interface
	return &Field{query: nil, fieldName: fieldName}
}

func (q *QueryV2) WithSQL(sql string) qdata.Query {
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

	q.sqlQuery = sql

	return q
}

func (q *QueryV2) Execute(ctx context.Context) []qdata.EntityBinding {
	// Parse SQL using vitess parser
	if q.stmt == nil {
		qlog.Error("No SQL statement provided")
		return nil
	}

	// Process the SQL AST to handle field indirection
	processed, err := q.processAST(q.stmt)
	if err != nil {
		qlog.Error("Failed to process SQL AST: %v", err)
		return nil
	}

	// Initialize SQLite database
	if err := q.initDB(); err != nil {
		qlog.Error("Failed to initialize SQLite: %v", err)
		return nil
	}
	defer q.db.Close()

	// Load all entities of the type into SQLite
	if err := q.loadEntities(ctx); err != nil {
		qlog.Error("Failed to load entities: %v", err)
		return nil
	}

	// Execute the processed query
	rows, err := q.db.Query(processed)
	if err != nil {
		qlog.Error("Failed to execute query: %v", err)
		return nil
	}
	defer rows.Close()

	// Convert results to EntityBindings
	return q.processResults(ctx, rows)
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
	// Create columns for this entity dynamically
	columnMap := make(map[string]struct{})

	// Get all field names from the schema
	schema := q.store.GetEntitySchema(ctx, q.entityType)
	if schema == nil {
		return fmt.Errorf("schema not found for entity type: %s", q.entityType)
	}

	// Add schema fields to column map
	for _, field := range schema.GetFields() {
		fieldName := field.GetFieldName()
		if _, exists := columnMap[fieldName]; !exists {
			columnMap[fieldName] = struct{}{}
		}
	}

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
		field := entity.GetField(fieldName)
		if field != nil && field.GetValue() != nil {
			cols = append(cols, q.sanitizeColumnName(fieldName))
			vals = append(vals, q.getFieldValue(field))
			placeholders = append(placeholders, "?")
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
		// Handle field indirection in column names
		name := expr.Name.String()
		if strings.Contains(name, "->") {
			expr.Name = sqlparser.NewIdentifierCI(q.sanitizeColumnName(name))
		}
		return expr, nil

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
