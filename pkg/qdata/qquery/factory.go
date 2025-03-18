package qquery

import "github.com/rqure/qlib/pkg/qdata"

// NewQuery creates a standard query
func NewQuery(store qdata.Store) qdata.Query {
	return New(store)
}

// NewQueryV2 creates an advanced SQL-based query
func NewQueryV2(store qdata.Store) qdata.Query {
	return NewV2(store)
}
