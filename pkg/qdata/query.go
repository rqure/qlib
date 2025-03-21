package qdata

import "context"

type ExecutableQuery interface {
	// Execute executes the query and returns the results
	Execute(ctx context.Context) []EntityBinding
}

// Query represents a database query (V1)
type Query interface {
	ExecutableQuery

	// ForType sets the entity type for the query
	ForType(t string) Query

	// From sets the entity type for the query (alias for ForType)
	From(t string) Query

	// Select selects fields to be included in the query result
	Select(fields ...string) Query

	// Where creates a field query for a specific field
	Where(fieldName string) FieldQuery
}

// QueryV2 represents an SQL-based query with pagination
type QueryV2 interface {
	// Prepare sets the SQL query string
	Prepare(sql string) QueryV2

	// Next fetches the next page of results
	// Returns false when no more results are available
	Next(ctx context.Context) bool

	// GetCurrentPage returns the current page of results
	GetCurrentPage() []EntityBinding

	// GetTotalCount returns the total number of results
	GetTotalCount() int

	// GetPageCount returns the total number of pages
	GetPageCount() int

	// GetCurrentPageNum returns the current page number
	GetCurrentPageNum() int

	// PageSize sets the number of items per page
	PageSize(size int) QueryV2
}

// FieldQuery represents a field condition in a query
type FieldQuery interface {
	Equals(value any) Query
	NotEquals(value any) Query
	GreaterThan(value any) Query
	LessThan(value any) Query
	LessThanOrEqual(value any) Query
	GreaterThanOrEqual(value any) Query
	Contains(value any) Query
	NotContains(value any) Query
}
