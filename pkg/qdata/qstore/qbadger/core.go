package qbadger

import (
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
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
	WithDB(ctx context.Context, fn func(txn *badger.Txn) error) error
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

// WithDB executes a function within a BadgerDB transaction
func (me *badgerCore) WithDB(ctx context.Context, fn func(txn *badger.Txn) error) error {
	if me.db == nil {
		return fmt.Errorf("badgerDB is not initialized")
	}

	// Use an update transaction to be safe
	return me.db.Update(func(txn *badger.Txn) error {
		return fn(txn)
	})
}

// WithWriteTxn executes a function within a BadgerDB write transaction
func (me *badgerCore) WithWriteTxn(ctx context.Context, fn func(txn *badger.Txn) error) error {
	if me.db == nil {
		return fmt.Errorf("badgerDB is not initialized")
	}

	return me.db.Update(fn)
}

// WithReadTxn executes a function within a BadgerDB read-only transaction
func (me *badgerCore) WithReadTxn(ctx context.Context, fn func(txn *badger.Txn) error) error {
	if me.db == nil {
		return fmt.Errorf("badgerDB is not initialized")
	}

	return me.db.View(fn)
}

// Close closes the BadgerDB connection
func (me *badgerCore) Close() error {
	if me.db != nil {
		return me.db.Close()
	}
	return nil
}
