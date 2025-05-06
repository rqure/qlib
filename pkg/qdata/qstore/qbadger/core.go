package qbadger

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// contextKey type for context values
type contextKey string

// Context keys
const (
	txnKey contextKey = "badger-txn"
)

// BadgerConfig holds BadgerDB connection parameters
type BadgerConfig struct {
	Path      string // Path for disk-based DB, empty for in-memory
	InMemory  bool   // Whether to use in-memory mode
	ValueDir  string // Optional separate directory for values (disk mode only)
	ValueSize int64  // Maximum size for inline values
}

// BadgerCore defines the interface for BadgerDB core functionality
type BadgerCore interface {
	SetDB(db *badger.DB)
	GetDB() *badger.DB
	SetConfig(config BadgerConfig)
	GetConfig() BadgerConfig
	WithWriteTxn(ctx context.Context, fn func(txn *badger.Txn) error) error
	WithReadTxn(ctx context.Context, fn func(txn *badger.Txn) error) error
	Close() error
}

type badgerCore struct {
	db     *badger.DB
	config BadgerConfig
}

// NewCore creates a new BadgerDB core with the given configuration
func NewCore(config BadgerConfig) BadgerCore {
	return &badgerCore{config: config}
}

func (me *badgerCore) SetDB(db *badger.DB) {
	me.db = db
}

func (me *badgerCore) GetDB() *badger.DB {
	return me.db
}

func (me *badgerCore) SetConfig(config BadgerConfig) {
	me.config = config
}

func (me *badgerCore) GetConfig() BadgerConfig {
	return me.config
}

// getTxnFromContext retrieves a transaction from context if it exists
func getTxnFromContext(ctx context.Context) *badger.Txn {
	if ctx == nil {
		return nil
	}

	txn, ok := ctx.Value(txnKey).(*badger.Txn)
	if !ok {
		return nil
	}
	return txn
}

// contextWithTxn adds a transaction to context
func contextWithTxn(ctx context.Context, txn *badger.Txn) context.Context {
	return context.WithValue(ctx, txnKey, txn)
}

// WithWriteTxn executes a function within a BadgerDB write transaction
func (me *badgerCore) WithWriteTxn(ctx context.Context, fn func(txn *badger.Txn) error) error {
	if me.db == nil {
		return fmt.Errorf("badgerDB is not initialized")
	}

	// Check if we already have a transaction in the context
	existingTxn := getTxnFromContext(ctx)
	if existingTxn != nil {
		// Reuse the existing transaction
		return fn(existingTxn)
	}

	// Create a new transaction
	return me.db.Update(func(txn *badger.Txn) error {
		// Add transaction to context for potential reuse in nested calls
		ctxWithTxn := contextWithTxn(ctx, txn)
		return fn(getTxnFromContext(ctxWithTxn))
	})
}

// WithReadTxn executes a function within a BadgerDB read-only transaction
func (me *badgerCore) WithReadTxn(ctx context.Context, fn func(txn *badger.Txn) error) error {
	if me.db == nil {
		return fmt.Errorf("badgerDB is not initialized")
	}

	// Check if we already have a transaction in the context
	existingTxn := getTxnFromContext(ctx)
	if existingTxn != nil {
		// Reuse the existing transaction
		return fn(existingTxn)
	}

	// Create a new read-only transaction
	return me.db.View(func(txn *badger.Txn) error {
		// Add transaction to context for potential reuse in nested calls
		ctxWithTxn := contextWithTxn(ctx, txn)
		return fn(getTxnFromContext(ctxWithTxn))
	})
}

// Close closes the BadgerDB connection
func (me *badgerCore) Close() error {
	if me.db != nil {
		return me.db.Close()
	}
	return nil
}
