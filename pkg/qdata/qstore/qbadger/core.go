package qbadger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rqure/qlib/pkg/qlog"
)

// contextKey type for context values
type contextKey string

// Context keys
const (
	txnKey contextKey = "badger-txn"
)

// BadgerConfig holds BadgerDB connection parameters
type BadgerConfig struct {
	Path                   string        // Path for disk-based DB, empty for in-memory
	InMemory               bool          // Whether to use in-memory mode
	ValueDir               string        // Optional separate directory for values (disk mode only)
	ValueSize              int64         // Maximum size for inline values
	GCInterval             time.Duration // Interval for garbage collection (defaults to 10 minutes if zero)
	SnapshotInterval       time.Duration // Interval for creating DB snapshots (defaults to 1 hour if zero)
	SnapshotRetention      int           // Number of snapshots to retain (defaults to 3 if zero)
	SnapshotDirectory      string        // Directory to store snapshots (defaults to "snapshots" subdirectory if empty)
	DisableBackgroundTasks bool          // Set to true to disable all background tasks
}

// BadgerCore defines the interface for BadgerDB core functionality
type BadgerCore interface {
	SetDB(db *badger.DB)
	GetDB() *badger.DB
	SetConfig(config BadgerConfig)
	GetConfig() BadgerConfig
	WithWriteTxn(ctx context.Context, fn func(txn *badger.Txn) error) error
	WithReadTxn(ctx context.Context, fn func(txn *badger.Txn) error) error
	StartBackgroundTasks(ctx context.Context)
	StopBackgroundTasks()
	Close() error
}

type badgerCore struct {
	db              *badger.DB
	config          BadgerConfig
	cancelFunctions []context.CancelFunc
}

// NewCore creates a new BadgerDB core with the given configuration
func NewCore(config BadgerConfig) BadgerCore {
	// Set defaults if not specified
	if config.GCInterval == 0 {
		config.GCInterval = 10 * time.Minute
	}
	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = 1 * time.Hour
	}
	if config.SnapshotRetention == 0 {
		config.SnapshotRetention = 3
	}
	if config.SnapshotDirectory == "" && !config.InMemory && config.Path != "" {
		config.SnapshotDirectory = filepath.Join(filepath.Dir(config.Path), "snapshots")
	}

	return &badgerCore{
		config:          config,
		cancelFunctions: make([]context.CancelFunc, 0),
	}
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

// StartBackgroundTasks starts periodic tasks for DB maintenance
func (me *badgerCore) StartBackgroundTasks(ctx context.Context) {
	me.StopBackgroundTasks()

	// Skip background tasks if in-memory or explicitly disabled
	if me.config.DisableBackgroundTasks || (me.config.InMemory && me.config.Path == "") {
		return
	}

	// Create snapshot directory if needed
	if me.config.Path != "" && me.config.SnapshotDirectory != "" {
		if err := os.MkdirAll(me.config.SnapshotDirectory, 0755); err != nil {
			qlog.Error("Failed to create snapshot directory: %v", err)
		}
	}

	// Start garbage collection task
	if me.config.GCInterval > 0 {
		gcCtx, gcCancel := context.WithCancel(ctx)
		me.cancelFunctions = append(me.cancelFunctions, gcCancel)
		go me.runGarbageCollection(gcCtx)
	}

	// Start snapshot task (only for disk-based DB)
	if !me.config.InMemory && me.config.Path != "" && me.config.SnapshotInterval > 0 {
		snapshotCtx, snapshotCancel := context.WithCancel(ctx)
		me.cancelFunctions = append(me.cancelFunctions, snapshotCancel)
		go me.runSnapshotTask(snapshotCtx)
	}
}

// StopBackgroundTasks stops all background tasks
func (me *badgerCore) StopBackgroundTasks() {
	for _, cancel := range me.cancelFunctions {
		cancel()
	}
	me.cancelFunctions = make([]context.CancelFunc, 0)
}

// runGarbageCollection runs periodic garbage collection
func (me *badgerCore) runGarbageCollection(ctx context.Context) {
	ticker := time.NewTicker(me.config.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if me.db == nil {
				continue
			}

			// Run garbage collection
			err := me.db.RunValueLogGC(0.5) // Run GC if space can be reclaimed
			if err != nil && err != badger.ErrNoRewrite {
				qlog.Warn("BadgerDB garbage collection error: %v", err)
			} else if err == nil {
				qlog.Trace("BadgerDB garbage collection completed successfully")
			}
		}
	}
}

// runSnapshotTask creates periodic snapshots of the database
func (me *badgerCore) runSnapshotTask(ctx context.Context) {
	ticker := time.NewTicker(me.config.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if me.db == nil || me.config.InMemory {
				continue
			}

			// Create snapshot filename with timestamp
			timestamp := time.Now().Format("20060102_150405")
			snapshotFile := filepath.Join(me.config.SnapshotDirectory, fmt.Sprintf("badger_snapshot_%s.bak", timestamp))

			// Create the file for writing
			file, err := os.Create(snapshotFile)
			if err != nil {
				qlog.Error("Failed to create snapshot file: %v", err)
				continue
			}

			// Create snapshot using the file as io.Writer
			_, err = me.db.Backup(file, 0)

			// Close the file regardless of backup success
			closeErr := file.Close()
			if closeErr != nil {
				qlog.Warn("Error closing snapshot file: %v", closeErr)
			}

			// Check if backup failed
			if err != nil {
				qlog.Error("Failed to create BadgerDB snapshot: %v", err)
				// Try to remove the incomplete snapshot file
				os.Remove(snapshotFile)
				continue
			}

			qlog.Info("BadgerDB snapshot created: %s", snapshotFile)

			// Cleanup old snapshots
			me.cleanupOldSnapshots()
		}
	}
}

// cleanupOldSnapshots removes old snapshots beyond retention limit
func (me *badgerCore) cleanupOldSnapshots() {
	if me.config.SnapshotRetention <= 0 || me.config.SnapshotDirectory == "" {
		return
	}

	// List snapshot files
	files, err := filepath.Glob(filepath.Join(me.config.SnapshotDirectory, "badger_snapshot_*.bak"))
	if err != nil {
		qlog.Error("Failed to list snapshot files: %v", err)
		return
	}

	// If we have more snapshots than retention limit, delete oldest
	if len(files) > me.config.SnapshotRetention {
		// Sort files by name (which includes timestamp)
		// This assumes filenames follow the pattern badger_snapshot_YYYYMMDD_HHMMSS.bak
		for i := 0; i < len(files)-me.config.SnapshotRetention; i++ {
			if err := os.Remove(files[i]); err != nil {
				qlog.Error("Failed to remove old snapshot %s: %v", files[i], err)
			} else {
				qlog.Info("Removed old snapshot: %s", files[i])
			}
		}
	}
}

// Close closes the BadgerDB connection and stops background tasks
func (me *badgerCore) Close() error {
	me.StopBackgroundTasks()

	if me.db != nil {
		return me.db.Close()
	}
	return nil
}
