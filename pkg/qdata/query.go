package qdata

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

func writerIdColumnName(columnName string) string {
	return fmt.Sprintf("%s$WriterId", columnName)
}

func writeTimeColumnName(columnName string) string {
	return fmt.Sprintf("%s$WriteTime", columnName)
}

type QueryColumn struct {
	ColumnName string
	Alias      string
	Table      QueryTable
	IsSelected bool
	Order      int // Add this field to track selection order
}

func (me *QueryColumn) FinalName() string {
	if me.Alias != "" {
		return me.Alias
	}

	return me.ColumnName
}

func (me *QueryColumn) FieldType() FieldType {
	columnName := strings.Split(me.ColumnName, ".")
	return FieldType(columnName[len(columnName)-1])
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
	Columns     map[string]QueryColumn // Changed from slice to map keyed by FinalName
	ColumnOrder []string
	Tables      map[string]QueryTable // Changed from slice to map keyed by FinalName
	OriginalSQL string
	From        sqlparser.TableExprs
	Where       *sqlparser.Where
	OrderBy     sqlparser.OrderBy
	GroupBy     sqlparser.GroupBy
	Having      *sqlparser.Where
}

type QueryRow interface {
	Get(column string) *Value
	Set(column string, value *Value, selected bool)
	Columns() []string
	Selected() []string
	SetOrder([]string)
	IsSelected(column string) bool
	AsQueryRowPb() *qprotobufs.QueryRow
	FromQueryRowPb(row *qprotobufs.QueryRow)
	AsEntity() *Entity
}

type orderedQueryRow struct {
	data        map[string]*Value
	selected    map[string]bool
	columnOrder []string
}

func NewQueryRow() QueryRow {
	return &orderedQueryRow{
		data:        make(map[string]*Value),
		selected:    make(map[string]bool),
		columnOrder: make([]string, 0),
	}
}

func (me *orderedQueryRow) Get(column string) *Value {
	return me.data[column]
}

func (me *orderedQueryRow) Set(column string, value *Value, selected bool) {
	if _, exists := me.data[column]; !exists {
		me.columnOrder = append(me.columnOrder, column)
	}
	me.data[column] = value
	me.selected[column] = selected
}

func (me *orderedQueryRow) Columns() []string {
	return me.columnOrder
}

func (me *orderedQueryRow) Selected() []string {
	selected := make([]string, 0)
	for _, k := range me.columnOrder {
		if me.selected[k] {
			selected = append(selected, k)
		}
	}
	return selected
}

func (me *orderedQueryRow) IsSelected(column string) bool {
	if _, ok := me.selected[column]; ok {
		return me.selected[column]
	}
	return false
}

func (me *orderedQueryRow) AsQueryRowPb() *qprotobufs.QueryRow {
	row := &qprotobufs.QueryRow{
		Columns: []*qprotobufs.QueryColumn{},
	}

	// Use columnOrder to maintain order
	for _, k := range me.columnOrder {
		if v, ok := me.data[k]; ok {
			row.Columns = append(row.Columns, &qprotobufs.QueryColumn{
				Key:        k,
				Value:      v.AsAnyPb(),
				IsSelected: me.selected[k],
			})
		}
	}

	return row
}

func (me *orderedQueryRow) FromQueryRowPb(row *qprotobufs.QueryRow) {
	me.data = make(map[string]*Value)
	me.selected = make(map[string]bool)
	me.columnOrder = make([]string, 0, len(row.Columns))

	for _, col := range row.Columns {
		me.columnOrder = append(me.columnOrder, col.Key)
		me.data[col.Key] = new(Value).FromAnyPb(col.Value)
		me.selected[col.Key] = col.IsSelected
	}
}

func (me *orderedQueryRow) AsEntity() *Entity {
	entity := new(Entity).Init(me.data["$EntityId"].GetEntityReference())

	for _, k := range me.columnOrder {
		v := me.data[k]
		if k == "$EntityId" || k == "$EntityType" || k == "$CursorId" {
			continue
		}

		if strings.Contains(k, "$WriterId") {
			entity.Field(FieldType(k)).WriterId = v.GetEntityReference()
		} else if strings.Contains(k, "$WriteTime") {
			entity.Field(FieldType(k)).WriteTime.FromTime(v.GetTimestamp())
		} else {
			entity.Field(FieldType(k)).Value.FromValue(v)
		}
	}

	return entity
}

func (me *orderedQueryRow) SetOrder(order []string) {
	original := me.columnOrder
	newOrder := make([]string, 0, len(order))
	missing := make([]string, 0, len(order))
	for _, v := range order {
		if !slices.Contains(original, v) {
			missing = append(missing, v)
			continue
		}
		newOrder = append(newOrder, v)
	}
	me.columnOrder = append(newOrder, missing...)
}

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

func ParseQuery(ctx context.Context, sql string, store StoreInteractor) (*ParsedQuery, error) {
	start := time.Now()
	qlog.Trace("ParseQuery: Parsing SQL: %s", sql)
	defer func() {
		qlog.Trace("ParseQuery: Total execution time: %v", time.Since(start))
	}()

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
		Columns:     make(map[string]QueryColumn),
		ColumnOrder: make([]string, 0),
		Tables:      make(map[string]QueryTable),
		OriginalSQL: sql,
		From:        selectStmt.From,
		Where:       selectStmt.Where,
		OrderBy:     selectStmt.OrderBy,
		GroupBy:     selectStmt.GroupBy,
		Having:      selectStmt.Having,
	}

	// Parse tables first since we need them for wildcard expansion
	tableLookup := make(map[string]QueryTable)
	for _, tableExpr := range selectStmt.From {
		processTableExpr(tableExpr, tableLookup, parsed.Tables)
	}

	// Parse fields from SELECT clause with wildcard support
	for _, expr := range selectStmt.SelectExprs {
		if starExpr, ok := expr.(*sqlparser.StarExpr); ok {
			// Handle * or table.*
			qualifier := starExpr.TableName.Name.String()
			if qualifier == "" {
				// SELECT * - expand for all tables
				for _, table := range parsed.Tables {
					if err := expandWildcard(ctx, table, parsed, store); err != nil {
						return nil, err
					}
				}
			} else {
				// SELECT table.* - expand for specific table
				if table, exists := tableLookup[qualifier]; exists {
					if err := expandWildcard(ctx, table, parsed, store); err != nil {
						return nil, err
					}
				}
			}
		} else {
			// Handle regular field expressions
			fields := extractFieldsFromExpr(expr, tableLookup, true)
			for _, field := range fields {
				finalName := field.FinalName()
				if _, exists := parsed.Columns[finalName]; !exists {
					parsed.Columns[finalName] = field
					parsed.ColumnOrder = append(parsed.ColumnOrder, finalName)
					qlog.Trace("ParseQuery: Parsed SELECT field: %s, alias: %s", field.FieldType(), field.Alias)
				}
			}
		}
	}

	// Extract fields from WHERE clause
	if selectStmt.Where != nil {
		fields := extractFieldsFromWhere(selectStmt.Where, tableLookup)
		for _, field := range fields {
			finalName := field.FinalName()
			if _, exists := parsed.Columns[finalName]; !exists {
				parsed.Columns[finalName] = field
				qlog.Trace("ParseQuery: Parsed WHERE field: %s", field.FieldType())
			}
		}
	}

	// Extract fields from GROUP BY clause
	for _, groupBy := range selectStmt.GroupBy {
		fields := extractFieldsFromExpr(groupBy, tableLookup, false)
		for _, field := range fields {
			finalName := field.FinalName()
			if _, exists := parsed.Columns[finalName]; !exists {
				parsed.Columns[finalName] = field
				qlog.Trace("ParseQuery: Parsed GROUP BY field: %s", field.FieldType())
			}
		}
	}

	// Extract fields from HAVING clause
	if selectStmt.Having != nil {
		fields := extractFieldsFromWhere(selectStmt.Having, tableLookup)
		for _, field := range fields {
			finalName := field.FinalName()
			if _, exists := parsed.Columns[finalName]; !exists {
				parsed.Columns[finalName] = field
				qlog.Trace("ParseQuery: Parsed HAVING field: %s", field.FieldType())
			}
		}
	}

	// Extract fields from ORDER BY clause
	for _, orderBy := range selectStmt.OrderBy {
		fields := extractFieldsFromExpr(orderBy.Expr, tableLookup, false)
		for _, field := range fields {
			finalName := field.FinalName()
			if _, exists := parsed.Columns[finalName]; !exists {
				parsed.Columns[finalName] = field
				qlog.Trace("ParseQuery: Parsed ORDER BY field: %s", field.FieldType())
			}
		}
	}

	qlog.Trace("ParseQuery: Successfully parsed query with %d fields", len(parsed.Columns))
	return parsed, nil
}

// expandWildcard adds all fields from the entity schema to the columns map
func expandWildcard(ctx context.Context, table QueryTable, parsed *ParsedQuery, store StoreInteractor) error {
	schema := store.GetEntitySchema(ctx, table.EntityType())
	if schema == nil {
		return fmt.Errorf("no schema found for entity type: %s", table.EntityType())
	}

	// Always include system columns
	systemColumns := []string{"$EntityId", "$EntityType"}
	for _, sysCol := range systemColumns {
		col := QueryColumn{
			ColumnName: sysCol,
			Table:      table,
			IsSelected: true,
		}
		finalName := col.FinalName()
		if _, exists := parsed.Columns[finalName]; !exists {
			parsed.Columns[finalName] = col
			parsed.ColumnOrder = append(parsed.ColumnOrder, col.ColumnName)
			qlog.Trace("expandWildcard: Added column %s to table %s", col.ColumnName, table.TableName)
		}
	}

	for fieldName := range schema.Fields {
		// Create a QueryColumn for each col
		col := QueryColumn{
			ColumnName: string(fieldName),
			Table:      table,
			IsSelected: true,
		}

		// Use FinalName as the key to avoid duplicates
		finalName := col.FinalName()
		if _, exists := parsed.Columns[finalName]; !exists {
			parsed.Columns[finalName] = col
			parsed.ColumnOrder = append(parsed.ColumnOrder, col.ColumnName)
			qlog.Trace("expandWildcard: Added column %s to table %s", col.ColumnName, table.TableName)
		}
	}

	return nil
}

// Expanded helper function to process both tables and JOIN conditions
func processTableExpr(expr sqlparser.TableExpr, tableLookup map[string]QueryTable, tables map[string]QueryTable) {
	switch node := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := sqlparser.String(node.Expr)
		entityType := strings.Trim(tableName, "\"")
		alias := node.As.String()
		if alias == "" {
			alias = entityType
		}
		queryTable := QueryTable{
			TableName: entityType,
			Alias:     alias,
		}
		tableLookup[alias] = queryTable
		tables[queryTable.FinalName()] = queryTable
		qlog.Trace("processTableExpr: Parsed table: %s with alias: %s", entityType, alias)

	case *sqlparser.JoinTableExpr:
		// Process both tables in the join
		processTableExpr(node.LeftExpr, tableLookup, tables)
		processTableExpr(node.RightExpr, tableLookup, tables)
		qlog.Trace("processTableExpr: Processed JOIN expression")

	case *sqlparser.ParenTableExpr:
		for _, tableExpr := range node.Exprs {
			processTableExpr(tableExpr, tableLookup, tables)
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
		columnName := strings.Trim(node.Name.String(), "\"")

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
	case *sqlparser.SQLVal:
		columnName := strings.Trim(string(node.Val), `"`)
		qualifier := ""

		sp := strings.Split(columnName, ".")
		if len(sp) > 1 {
			qualifier = sp[0]
			columnName = sp[1]
		}

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

type ExprEvaluator struct {
	store     StoreInteractor
	parsed    *ParsedQuery
	program   *vm.Program
	typeHints TypeHintMap
}

func NewExprEvaluator(store StoreInteractor, parsed *ParsedQuery) *ExprEvaluator {
	typeHints := make(TypeHintMap)
	typeHints["$CursorId"] = VTInt
	typeHints["$EntityId"] = VTEntityReference
	typeHints["$EntityType"] = VTString

	return &ExprEvaluator{
		store:     store,
		parsed:    parsed,
		program:   nil,
		typeHints: typeHints,
	}
}

func (me *ExprEvaluator) CanEvaluate() bool {
	start := time.Now()
	qlog.Trace("ExprEvaluator.CanEvaluate: Checking if expression can be evaluated")
	defer func() {
		qlog.Trace("ExprEvaluator.CanEvaluate: Total execution time: %v", time.Since(start))
	}()

	if me.parsed == nil {
		return false
	}

	if me.parsed.GroupBy != nil {
		return false
	}

	if me.parsed.Having != nil {
		return false
	}

	if me.parsed.OrderBy != nil {
		return false
	}

	if len(me.parsed.Tables) > 1 {
		return false
	}

	exprStr := `true`
	if me.parsed.Where != nil {
		whereClause := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(
			strings.TrimSpace(sqlparser.String(me.parsed.Where)), "where"), "WHERE"))

		// Convert SQL operators to expr language operators
		exprStr = convertSQLToExpr(whereClause)
		qlog.Trace("CanEvaluate: Converted SQL WHERE to expr: %s", exprStr)
	}

	var err error
	me.program, err = expr.Compile(exprStr)
	if err != nil {
		qlog.Trace("CanEvaluate: Failed to create evaluable expression: %v", err)
		return false
	}

	return true
}

// convertSQLToExpr converts SQL syntax to expr language syntax
func convertSQLToExpr(sqlExpr string) string {
	start := time.Now()
	qlog.Trace("convertSQLToExpr: Converting SQL expression: %s", sqlExpr)
	defer func() {
		qlog.Trace("convertSQLToExpr: Conversion completed in %v", time.Since(start))
	}()

	// Replace common SQL operators with expr language operators
	replacements := map[string]string{
		" AND ":        " && ",
		" OR ":         " || ",
		"NOT ":         "!",
		" NOT ":        " !",
		" = ":          " == ",
		" IS NULL":     " == nil",
		" IS NOT NULL": " != nil",
		"NULL":         "nil",
		" LIKE ":       " matches ",
	}

	result := sqlExpr

	// Handle complex LIKE patterns by converting them to regex patterns
	// This regex finds LIKE operators and their patterns
	likeRegex := regexp.MustCompile(`\s+LIKE\s+['"](.+?)['"]`)
	result = likeRegex.ReplaceAllStringFunc(result, func(match string) string {
		// Extract the pattern
		submatches := likeRegex.FindStringSubmatch(match)
		if len(submatches) < 2 {
			return match
		}

		pattern := submatches[1]
		// Convert SQL LIKE pattern to regex pattern
		pattern = strings.Replace(pattern, "%", ".*", -1)
		pattern = strings.Replace(pattern, "_", ".", -1)
		return ` matches "^` + pattern + `$"`
	})

	// Apply simple replacements
	for sqlOp, exprOp := range replacements {
		result = strings.Replace(result, sqlOp, exprOp, -1)
	}

	// Handle case-insensitive comparison for SQL
	result = strings.Replace(result, "LOWER(", "lower(", -1)
	result = strings.Replace(result, "UPPER(", "upper(", -1)

	// Handle function names
	result = strings.Replace(result, "COUNT(", "count(", -1)
	result = strings.Replace(result, "SUM(", "sum(", -1)
	result = strings.Replace(result, "AVG(", "mean(", -1)
	result = strings.Replace(result, "MIN(", "min(", -1)
	result = strings.Replace(result, "MAX(", "max(", -1)

	// Handle IN operator
	inRegex := regexp.MustCompile(`(\w+)\s+IN\s+\(([^)]+)\)`)
	result = inRegex.ReplaceAllStringFunc(result, func(match string) string {
		submatches := inRegex.FindStringSubmatch(match)
		if len(submatches) < 3 {
			return match
		}

		field := submatches[1]
		items := submatches[2]
		// Convert to expr's "in" operator syntax
		return field + " in [" + items + "]"
	})

	// Handle BETWEEN operator
	betweenRegex := regexp.MustCompile(`(\w+)\s+BETWEEN\s+(\w+)\s+AND\s+(\w+)`)
	result = betweenRegex.ReplaceAllStringFunc(result, func(match string) string {
		submatches := betweenRegex.FindStringSubmatch(match)
		if len(submatches) < 4 {
			return match
		}

		field := submatches[1]
		lower := submatches[2]
		upper := submatches[3]
		// Convert to expr's comparison operators
		return field + " >= " + lower + " && " + field + " <= " + upper
	})

	return result
}

func (me *ExprEvaluator) ExecuteWithPagination(ctx context.Context, pageSize int64, cursorId int64, opts ...TypeHintOpts) (*PageResult[QueryRow], error) {
	start := time.Now()
	qlog.Trace("ExprEvaluator.ExecuteWithPagination: Starting for pageSize %d, cursorId %d", pageSize, cursorId)
	defer func() {
		qlog.Trace("ExprEvaluator.ExecuteWithPagination: Total execution time: %v", time.Since(start))
	}()

	if me.program == nil {
		return &PageResult[QueryRow]{
			Items:    []QueryRow{},
			CursorId: -1,
			NextPage: nil,
		}, fmt.Errorf("expression is not initialized")
	}

	for _, opt := range opts {
		opt(me.typeHints)
	}

	// Set a reasonable default for page size if it's not positive
	if pageSize <= 0 {
		pageSize = 100
	}

	rows := make([]QueryRow, 0)
	var lastSeenCursorId int64 = -1

	for tableName := range me.parsed.Tables {
		tableStart := time.Now()
		qlog.Trace("ExprEvaluator.ExecuteWithPagination: Processing table %s with cursorId %d", tableName, cursorId)
		defer func(tableName string) {
			qlog.Trace("ExprEvaluator.ExecuteWithPagination: Finished processing table %s in %v",
				tableName, time.Since(tableStart))
		}(tableName)

		// Get the entities using pagination
		pageResult := me.store.FindEntities(EntityType(tableName), POCursorId(cursorId), POPageSize(pageSize))
		defer pageResult.Close() // Ensure we close the page result when done

		lastSeenCursorId = pageResult.CursorId
		if len(pageResult.Items) == 0 && pageResult.CursorId >= 0 {
			var err error
			pageResult, err = pageResult.NextPage(ctx)
			if err != nil {
				return &PageResult[QueryRow]{
					Items:    []QueryRow{},
					CursorId: -1,
					NextPage: nil,
				}, err
			}
		}

		// Process entities from the page result
		entityCount := 0
		matchCount := 0
		evalStart := time.Now()

		for _, entityId := range pageResult.Items {
			entityCount++
			entityStart := time.Now()

			requests := make([]*Request, 0, len(me.parsed.Columns))
			for _, col := range me.parsed.Columns {
				if strings.Contains(col.ColumnName, "$") {
					continue
				}

				ft := col.FieldType()
				requests = append(requests, new(Request).Init(entityId, ft))
			}

			// Read all field data for this entity
			me.store.Read(ctx, requests...)

			// Create a new query row
			row := NewQueryRow()

			// Add system fields
			{
				isSelected := false
				if col, ok := me.parsed.Columns["$EntityId"]; ok {
					isSelected = col.IsSelected
				}
				row.Set("$EntityId", NewEntityReference(entityId), isSelected)
			}

			{
				isSelected := false
				if col, ok := me.parsed.Columns["$EntityType"]; ok {
					isSelected = col.IsSelected
				}
				row.Set("$EntityType", NewString(entityId.GetEntityType().AsString()), isSelected)
			}

			{
				// Add cursor ID for pagination - using entity's cursor ID
				row.Set("$CursorId", NewInt(int(lastSeenCursorId)), false)
			}

			// Add data from field requests
			for _, req := range requests {
				if !req.Success {
					qlog.Trace("ExecuteWithPagination: Failed to read field %s for entity %s", req.FieldType, req.EntityId)
					continue
				}

				columnName := string(req.FieldType)
				isSelected := false
				if col, ok := me.parsed.Columns[columnName]; ok {
					isSelected = col.IsSelected
					columnName = col.FinalName()
				}

				row.Set(columnName, req.Value, isSelected)

				// Add writer information if needed
				isSelected = false
				writerIdColumn := writerIdColumnName(columnName)
				if _, ok := me.typeHints[writerIdColumn]; !ok {
					me.typeHints[writerIdColumn] = VTEntityReference
				}
				if _, ok := me.parsed.Columns[writerIdColumn]; ok {
					isSelected = me.parsed.Columns[writerIdColumn].IsSelected
				}
				row.Set(writerIdColumn, me.typeHints[writerIdColumn].NewValue(req.WriterId), isSelected)

				isSelected = false
				writeTimeColumn := writeTimeColumnName(columnName)
				if _, ok := me.typeHints[writeTimeColumn]; !ok {
					me.typeHints[writeTimeColumn] = VTTimestamp
				}
				if _, ok := me.parsed.Columns[writeTimeColumn]; ok {
					isSelected = me.parsed.Columns[writeTimeColumn].IsSelected
				}
				row.Set(writeTimeColumn, me.typeHints[writeTimeColumn].NewValue(req.WriteTime), isSelected)
			}

			// Evaluate the expression against the row
			params := make(map[string]interface{})
			for _, col := range row.Columns() {
				params[col] = row.Get(col).GetRaw()
			}

			evalExprStart := time.Now()
			result, err := expr.Run(me.program, params)
			if err != nil {
				qlog.Trace("ExprEvaluator.ExecuteWithPagination: Failed to evaluate expression: %v", err)
				continue
			}
			qlog.Trace("ExprEvaluator.ExecuteWithPagination: Expression evaluation took %v",
				time.Since(evalExprStart))

			// If expression evaluates to true, add the row to results
			if boolResult, ok := result.(bool); ok && boolResult {
				rows = append(rows, row)
				matchCount++
			}

			qlog.Trace("ExprEvaluator.ExecuteWithPagination: Processed entity in %v", time.Since(entityStart))
		}

		qlog.Trace("ExprEvaluator.ExecuteWithPagination: Evaluated %d entities, found %d matches in %v",
			entityCount, matchCount, time.Since(evalStart))
	}

	// Reference to this evaluator to ensure it's not garbage collected
	evaluator := me

	// Create PageResult with next page function
	result := &PageResult[QueryRow]{
		Items:    rows,
		CursorId: lastSeenCursorId,
		NextPage: func(ctx context.Context) (*PageResult[QueryRow], error) {
			if lastSeenCursorId < 0 {
				qlog.Trace("NextPage: No more results to fetch")
				return &PageResult[QueryRow]{
					Items:    []QueryRow{},
					CursorId: -1,
					NextPage: nil,
				}, nil
			}
			qlog.Trace("NextPage: Fetching next page with cursorId: %d", lastSeenCursorId)
			return evaluator.ExecuteWithPagination(ctx, pageSize, lastSeenCursorId)
		},
	}

	return result, nil
}

type SQLiteBuilder struct {
	db        *sql.DB
	store     StoreInteractor
	typeHints TypeHintMap
	closed    bool // Track if the builder has been closed
}

func NewSQLiteBuilder(store StoreInteractor) (*SQLiteBuilder, error) {
	start := time.Now()
	qlog.Trace("NewSQLiteBuilder: Creating new SQLite builder")
	defer func() {
		qlog.Trace("NewSQLiteBuilder: Total execution time: %v", time.Since(start))
	}()

	db, err := sql.Open("sqlite3", "")
	if err != nil {
		qlog.Trace("NewSQLiteBuilder: Failed to open in-memory SQLite database: %v", err)
		return nil, err
	}

	qlog.Trace("NewSQLiteBuilder: Successfully created SQLite in-memory database")

	typeHints := make(TypeHintMap)
	typeHints["$CursorId"] = VTInt
	typeHints["$EntityId"] = VTEntityReference
	typeHints["$EntityType"] = VTString

	return &SQLiteBuilder{
		db:        db,
		store:     store,
		typeHints: typeHints,
	}, nil
}

// Close releases resources used by the SQLiteBuilder
func (me *SQLiteBuilder) Close() error {
	if me.closed {
		return nil
	}

	qlog.Trace("SQLiteBuilder.Close: Cleaning up resources")

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

// buildAndPopulateTables creates tables for each entity type and populates them with data
func (me *SQLiteBuilder) buildAndPopulateTables(ctx context.Context, entityTypes []EntityType, query *ParsedQuery) error {
	start := time.Now()
	qlog.Trace("buildAndPopulateTables: Building tables for %d entity types", len(entityTypes))
	defer func() {
		qlog.Trace("buildAndPopulateTables: Total execution time: %v", time.Since(start))
	}()

	for _, entityType := range entityTypes {
		qlog.Trace("buildAndPopulateTables: Creating table for entity type %s", entityType)

		if err := me.buildTableForEntityType(ctx, entityType, query); err != nil {
			return fmt.Errorf("failed to build table for %s: %v", entityType, err)
		}

		if err := me.populateTableForEntityType(ctx, entityType, query); err != nil {
			return fmt.Errorf("failed to populate table for %s: %v", entityType, err)
		}
	}

	return nil
}

// buildTableForEntityType creates a table for the specified entity type
func (me *SQLiteBuilder) buildTableForEntityType(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	start := time.Now()
	defer func() {
		qlog.Trace("buildTableForEntityType: Built table for %s in %v", entityType, time.Since(start))
	}()

	// Drop the table if it exists
	_, err := me.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS \"%s\"", entityType))
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %v", err)
	}

	// Create table with all necessary columns
	columns := make([]string, 0)
	addedColumns := make(map[string]bool)
	columns = append(columns, "\"$EntityId\" TEXT PRIMARY KEY")
	addedColumns["$EntityId"] = true
	columns = append(columns, "\"$EntityType\" TEXT")
	addedColumns["$EntityType"] = true

	// Iterate through the columns map
	for _, col := range query.Columns {
		var colType string

		if addedColumns[col.ColumnName] {
			continue
		}

		if strings.Contains(col.ColumnName, "$") {
			// skip system columns
			continue
		}

		if col.Table.EntityType() != entityType {
			// skip columns not belonging to this entity type
			continue
		}

		finalName := col.FinalName()
		ft := col.FieldType()
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
			writerId := writerIdColumnName(col.ColumnName)
			writeTime := writeTimeColumnName(col.ColumnName)

			columns = append(columns, fmt.Sprintf("%q %s", col.ColumnName, colType))
			columns = append(columns, fmt.Sprintf("%q TEXT", writerId))
			columns = append(columns, fmt.Sprintf("%q DATETIME", writeTime))

			if _, ok := me.typeHints[writerId]; !ok {
				me.typeHints[writerId] = VTEntityReference
			}

			if _, ok := me.typeHints[writeTime]; !ok {
				me.typeHints[writeTime] = VTTimestamp
			}
		}
	}

	createSQL := fmt.Sprintf(`CREATE TABLE "%s" (%s)`, entityType, strings.Join(columns, ", "))
	qlog.Trace("Creating table with SQL: %s", createSQL)

	_, err = me.db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	qlog.Trace("buildTableForEntityType: Successfully created table [%s]", entityType)
	return nil
}

// populateTableForEntityType populates the table for the specified entity type
func (me *SQLiteBuilder) populateTableForEntityType(ctx context.Context, entityType EntityType, query *ParsedQuery) error {
	start := time.Now()
	qlog.Trace("populateTableForEntityType: Populating table for entity type %s", entityType)
	defer func() {
		qlog.Trace("populateTableForEntityType: Populated table %s in %v", entityType, time.Since(start))
	}()

	// Begin a transaction for batch inserts
	tx, err := me.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Prepare the insert statement for entity IDs
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		`INSERT OR IGNORE INTO "%s" ("$EntityId", "$EntityType") VALUES (?, ?)`,
		entityType))
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Collect entity IDs and insert them
	entityIds := make([]EntityId, 0)
	me.store.FindEntities(entityType).ForEach(ctx, func(entityId EntityId) bool {
		entityIds = append(entityIds, entityId)

		// Insert the entity ID
		if _, err := stmt.ExecContext(ctx, entityId, entityType); err != nil {
			qlog.Warn("populateTableForEntityType: Failed to insert entity %s: %v", entityId, err)
			return false
		}
		return true
	})

	// Commit transaction to ensure IDs are saved
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit entity IDs: %v", err)
	}

	qlog.Trace("Inserted %d entity IDs for type %s", len(entityIds), entityType)

	// Now load field data in bulk for all entities
	if len(entityIds) > 0 {
		if err := me.loadFieldDataForEntities(ctx, entityType, entityIds, query); err != nil {
			return fmt.Errorf("failed to load query fields: %v", err)
		}
	} else {
		qlog.Trace("No entity IDs found for type %s", entityType)
	}

	return nil
}

// loadFieldDataForEntities loads all field data for the given entities into their respective table
func (me *SQLiteBuilder) loadFieldDataForEntities(ctx context.Context, entityType EntityType, entityIds []EntityId, query *ParsedQuery) error {
	start := time.Now()
	qlog.Trace("loadFieldDataForEntities: Loading fields for %d entities of type %s", len(entityIds), entityType)
	defer func() {
		qlog.Trace("loadFieldDataForEntities: Loaded fields for %s in %v", entityType, time.Since(start))
	}()

	if len(entityIds) == 0 {
		return nil
	}

	// Create read requests for all fields of all entities
	allRequests := make([]*Request, 0)

	for _, entityId := range entityIds {
		entity := new(Entity).Init(entityId)
		// Iterate over columns map
		for _, field := range query.Columns {
			if strings.Contains(field.ColumnName, "$") {
				// Skip system columns
				continue
			}

			ft := field.FieldType()
			allRequests = append(allRequests, entity.Field(ft).AsReadRequest())
		}
	}

	qlog.Trace("loadFieldDataForEntities: Batch reading %d field requests", len(allRequests))
	// Execute all read requests in a batch
	me.store.Read(ctx, allRequests...)

	// Begin a new transaction for database updates
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
	for _, req := range allRequests {
		if !req.Success {
			continue
		}

		fieldType := req.FieldType
		entityId := req.EntityId

		// Find the corresponding field in the query by field type
		var queryField *QueryColumn
		for _, field := range query.Columns {
			if field.FieldType() == fieldType {
				queryFieldCopy := field
				queryField = &queryFieldCopy
				break
			}
		}

		if queryField == nil {
			continue
		}

		// Update field value in the appropriate table
		_, err = tx.ExecContext(ctx, fmt.Sprintf(`
			UPDATE "%s" SET "%s" = ?, "%s$WriterId" = ?, "%s$WriteTime" = ? WHERE "$EntityId" = ?
		`, entityType, queryField.ColumnName, queryField.ColumnName, queryField.ColumnName),
			convertValueForSQLite(req.Value),
			req.WriterId.AsString(),
			req.WriteTime.AsTime(),
			entityId)
		if err != nil {
			return fmt.Errorf("failed to update entity field: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit field updates: %v", err)
	}

	qlog.Trace("loadFieldDataForEntities: Successfully updated fields for %s", entityType)
	return nil
}

// executeQuery executes the given query against the entity tables and populates the final results table
func (me *SQLiteBuilder) executeQuery(ctx context.Context, query *ParsedQuery, entityTables []EntityType) error {
	start := time.Now()
	qlog.Trace("executeQuery: Creating final results table")
	defer func() {
		qlog.Trace("executeQuery: Total execution time: %v", time.Since(start))
	}()

	// Drop the final results table if it exists
	_, err := me.db.ExecContext(ctx, "DROP TABLE IF EXISTS final_results")
	if err != nil {
		return fmt.Errorf("failed to drop final results table: %v", err)
	}

	// Create the final results table - only include selected fields
	columns := []string{"[$CursorId] INTEGER PRIMARY KEY AUTOINCREMENT"}
	for _, field := range query.Columns {
		finalName := field.FinalName()
		vt, ok := me.typeHints[finalName]
		if ok {
			sqlType := getSQLiteType(vt)
			columns = append(columns, fmt.Sprintf(`"%s" %s`, finalName, sqlType))
		} else {
			columns = append(columns, fmt.Sprintf(`"%s" TEXT`, finalName))
		}
	}

	createSQL := fmt.Sprintf("CREATE TABLE final_results (%s)", strings.Join(columns, ", "))
	_, err = me.db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to create final results table: %v", err)
	}

	// Create an index on the cursor column for efficient WHERE-based pagination
	_, err = me.db.ExecContext(ctx, `CREATE INDEX idx_cursor ON final_results ("$CursorId")`)
	if err != nil {
		return fmt.Errorf("failed to create cursor index: %v", err)
	}

	// Build column names for the insert - only selected fields
	colNames := make([]string, 0, len(query.ColumnOrder))
	for _, columnName := range query.ColumnOrder {
		field, exists := query.Columns[columnName]
		if !exists {
			qlog.Warn("executeQuery: Field %s not found in query columns", columnName)
			continue
		}
		colNames = append(colNames, fmt.Sprintf(`"%s"`, field.FinalName()))
	}

	// Build the SELECT clause for the query - only selected fields
	selectFields := make([]string, 0, len(query.ColumnOrder))
	for _, columnName := range query.ColumnOrder {
		field, exists := query.Columns[columnName]
		if !exists {
			qlog.Warn("executeQuery: Field %s not found in query columns", columnName)
			continue
		}
		finalName := field.FinalName()
		selectFields = append(selectFields, fmt.Sprintf(`"%s" as "%s"`, field.ColumnName, finalName))
	}

	// Build the query for this entity table
	fromClause := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(
		strings.TrimSpace(sqlparser.String(query.From)), "from"), "FROM"))
	sqlQuery := fmt.Sprintf(`SELECT %s FROM %s`, strings.Join(selectFields, ", "), fromClause)

	if query.Where != nil {
		whereClause := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(
			strings.TrimSpace(sqlparser.String(query.Where)), "where"), "WHERE"))
		sqlQuery += " WHERE " + whereClause
	}

	if len(query.GroupBy) > 0 {
		groupBy := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(
			strings.TrimSpace(sqlparser.String(query.GroupBy)), "group by"), "GROUP BY"))
		sqlQuery += " GROUP BY " + groupBy
	}

	if query.Having != nil {
		havingClause := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(
			strings.TrimSpace(sqlparser.String(query.Having)), "having"), "HAVING"))
		sqlQuery += " HAVING " + havingClause
	}

	// Insert into final results table
	insertSQL := fmt.Sprintf(`INSERT INTO final_results (%s) %s`,
		strings.Join(colNames, ", "), sqlQuery)
	qlog.Trace("executeQuery: Executing query: %s", insertSQL)

	_, err = me.db.ExecContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	// Drop the temporary entity tables to free up memory
	for _, tableName := range entityTables {
		dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)
		_, err = me.db.ExecContext(ctx, dropSQL)
		if err != nil {
			qlog.Warn("executeQuery: Failed to drop temporary table [%s]: %v", tableName, err)
			// Continue with other tables instead of failing
			continue
		}
		qlog.Trace("executeQuery: Dropped temporary table [%s]", tableName)
	}

	return nil
}

// getPageFromResults fetches a specific page from the final results table
func (me *SQLiteBuilder) getPageFromResults(ctx context.Context, pageSize int64, cursorId int64) (*sql.Rows, error) {
	var query string
	if cursorId == 0 {
		// First page - no cursor filtering needed
		query = fmt.Sprintf(`SELECT * FROM final_results ORDER BY "$CursorId" LIMIT %d`, pageSize)
	} else {
		// Subsequent pages - use WHERE clause for better performance
		query = fmt.Sprintf(`SELECT * FROM final_results WHERE "$CursorId" > %d ORDER BY "$CursorId" LIMIT %d`,
			cursorId, pageSize)
	}

	rows, err := me.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get page: %v", err)
	}

	return rows, nil
}

// getEntityTypesFromQuery extracts all unique entity types referenced in the query
func getEntityTypesFromQuery(query *ParsedQuery) []EntityType {
	entityTypes := make(map[EntityType]bool)

	// Extract entity types from table references
	for _, table := range query.Tables {
		entityTypes[table.EntityType()] = true
	}

	result := make([]EntityType, 0, len(entityTypes))
	for entityType := range entityTypes {
		result = append(result, entityType)
	}

	return result
}

func (me *SQLiteBuilder) QueryWithPagination(ctx context.Context, query *ParsedQuery, pageSize int64, cursorId int64, opts ...TypeHintOpts) (*PageResult[QueryRow], error) {
	start := time.Now()
	qlog.Trace("QueryWithPagination: Starting for pageSize %d, cursorId %d", pageSize, cursorId)
	defer func() {
		qlog.Trace("QueryWithPagination: Total execution time: %v", time.Since(start))
	}()

	// Apply type hints
	for _, opt := range opts {
		opt(me.typeHints)
	}

	// Set a reasonable default for page size if it's not positive
	if pageSize <= 0 {
		pageSize = 100
	}

	// Only build and populate the tables on the first request (cursorId == 0)
	if cursorId == 0 {
		// Get all entity types referenced in the query
		entityTypes := getEntityTypesFromQuery(query)

		// Build and populate tables for all entity types
		if err := me.buildAndPopulateTables(ctx, entityTypes, query); err != nil {
			return nil, fmt.Errorf("failed to build and populate tables: %v", err)
		}

		// Execute the query and store results
		if err := me.executeQuery(ctx, query, entityTypes); err != nil {
			return nil, fmt.Errorf("failed to execute query: %v", err)
		}
	}

	// Get the requested page from the final results table
	rows, err := me.getPageFromResults(ctx, pageSize, cursorId)
	if err != nil {
		return nil, fmt.Errorf("failed to get page: %v", err)
	}
	defer rows.Close()

	// Convert results to QueryRows
	var queryRows []QueryRow
	for rows.Next() {
		row, err := me.rowToQueryRow(rows, query)
		if err != nil {
			qlog.Error("QueryWithPagination: Failed to convert row: %v", err)
			continue
		}
		queryRows = append(queryRows, row)
	}

	// Get the last cursor ID from the page to use as the next cursor
	var lastCursorId int64 = -1
	if len(queryRows) > 0 {
		// Extract the cursor ID from the last row
		lastRow := queryRows[len(queryRows)-1]
		if lastRow != nil {
			lastCursorId = int64(lastRow.Get("$CursorId").GetInt())
		}
	}

	// Create a reference to this builder to close it later
	builderRef := me

	// Create PageResult with next page function
	result := &PageResult[QueryRow]{
		Items:    queryRows,
		CursorId: lastCursorId,
		NextPage: func(ctx context.Context) (*PageResult[QueryRow], error) {
			if lastCursorId < 0 {
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
			qlog.Trace("NextPage: Fetching next page with cursorId: %d", lastCursorId)
			return me.QueryWithPagination(ctx, query, pageSize, lastCursorId, opts...)
		},
		Cleanup: me.Close,
	}

	return result, nil
}

// rowToQueryRow converts a database row to a QueryRow
func (me *SQLiteBuilder) rowToQueryRow(rows *sql.Rows, query *ParsedQuery) (QueryRow, error) {
	// Get column names from the rows
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %v", err)
	}

	// Create slices to hold the values
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Scan the row into the values slice
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %v", err)
	}

	queryRow := NewQueryRow()

	// Process each column
	for i, value := range values {
		if value == nil {
			continue
		}

		// Trim square brackets from column name if present
		columnName := strings.Trim(columns[i], "\"")
		vt, ok := me.typeHints[columnName]
		if !ok {
			vt = VTString // Default to string if no type hint is provided
		}

		isSelected := false
		queryCol, ok := query.Columns[columnName]
		if ok {
			isSelected = queryCol.IsSelected
		} else if columnName != "$CursorId" {
			qlog.Warn("rowToQueryRow: Column %s not found in query columns", columnName)
		}

		queryRow.Set(columnName, vt.NewValue(value), isSelected)
	}

	queryRow.SetOrder(query.ColumnOrder)

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

func convertValueForSQLite(value *Value) any {
	if value == nil {
		qlog.Trace("convertValueForSQLite: Received nil value")
		return nil
	}

	var result any
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
