package qdata

import "context"

type QuerySetup interface {
	// PageSize sets the number of items per page
	WithPageSize(size int) QuerySetup

	Prepare() Query
}

type Query interface {
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
}
