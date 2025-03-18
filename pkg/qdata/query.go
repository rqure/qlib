package qdata

import "context"

type ExecutableQuery interface {
	// Execute executes the query and returns the results
	Execute(ctx context.Context) []EntityBinding
}

// Query represents a database query
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

type QueryV2 interface {
	ExecutableQuery

	WithSQL(sql string) Query
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
