package qbadger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
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
	Connect(ctx context.Context)
	Disconnect(ctx context.Context)
	IsConnected() bool
	Connected() qss.Signal[qdata.ConnectedArgs]
	Disconnected() qss.Signal[qdata.DisconnectedArgs]
}

type badgerCore struct {
	db              *badger.DB
	config          BadgerConfig
	cancelFunctions []context.CancelFunc
	connected       qss.Signal[qdata.ConnectedArgs]
	disconnected    qss.Signal[qdata.DisconnectedArgs]
	isConnected     bool
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
		connected:       qss.New[qdata.ConnectedArgs](),
		disconnected:    qss.New[qdata.DisconnectedArgs](),
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

// Connected returns the signal that is emitted when connected
func (me *badgerCore) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

// Disconnected returns the signal that is emitted when disconnected
func (me *badgerCore) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}

// IsConnected returns whether the database is connected
func (me *badgerCore) IsConnected() bool {
	return me.isConnected
}

// Connect establishes a connection to the database
func (me *badgerCore) Connect(ctx context.Context) {
	if me.IsConnected() {
		return
	}

	// Close any existing database connection
	me.Close()

	var options badger.Options

	if me.config.InMemory {
		options = badger.DefaultOptions("").WithInMemory(true)
	} else {
		options = badger.DefaultOptions(me.config.Path)
		if me.config.ValueDir != "" {
			options = options.WithValueDir(me.config.ValueDir)
		}
	}

	if me.config.ValueSize > 0 {
		options = options.WithValueLogFileSize(me.config.ValueSize)
	}

	if me.config.SnapshotDirectory != "" {
		latestSnapshot := findLatestSnapshot(me.config.SnapshotDirectory)
		if latestSnapshot != "" {
			qlog.Info("Found database snapshot: %s", latestSnapshot)

			// For disk-based DB, create directory if it doesn't exist
			if !me.config.InMemory && me.config.Path != "" && !fileExists(me.config.Path) {
				if err := os.MkdirAll(filepath.Dir(me.config.Path), 0755); err != nil {
					qlog.Error("Failed to create database directory: %v", err)
				}
			}

			// Open the database (either in memory or disk-based)
			db, err := badger.Open(options)
			if err == nil {
				me.db = db

				file, err := os.Open(latestSnapshot)
				if err != nil {
					qlog.Error("Failed to open snapshot file: %v", err)
					_ = db.Close()
					me.db = nil
				} else {
					qlog.Info("Loading database from snapshot: %s", latestSnapshot)
					err = db.Load(file, 1) // 1 worker thread

					if closeErr := file.Close(); closeErr != nil {
						qlog.Warn("Error closing snapshot file: %v", closeErr)
					}

					if err != nil {
						qlog.Error("Failed to load database from snapshot: %v", err)
						_ = db.Close()
						me.db = nil
					} else {
						qlog.Info("Database loaded successfully from snapshot")
						me.isConnected = true
						me.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
						me.StartBackgroundTasks(ctx)
						return
					}
				}
			}
		}
	}

	// Open the database normally
	db, err := badger.Open(options)
	if err != nil {
		qlog.Error("Failed to connect to BadgerDB: %v", err)
		me.isConnected = false
		me.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx, Err: err})
		return
	}

	me.db = db
	me.isConnected = true
	me.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})

	// Start background tasks
	me.StartBackgroundTasks(ctx)
}

// Disconnect closes the database connection
func (me *badgerCore) Disconnect(ctx context.Context) {
	if !me.IsConnected() {
		return
	}

	// Take a final snapshot before disconnecting, but only if DB is initialized
	// and we have a snapshot directory configured
	if me.db != nil && !me.config.DisableBackgroundTasks && me.config.SnapshotDirectory != "" {
		qlog.Info("Taking final snapshot before disconnecting...")

		if err := os.MkdirAll(me.config.SnapshotDirectory, 0755); err != nil {
			qlog.Error("Failed to create snapshot directory: %v", err)
		} else {
			timestamp := time.Now().Format("20060102_150405")
			snapshotFile := filepath.Join(me.config.SnapshotDirectory, fmt.Sprintf("badger_snapshot_%s.bak", timestamp))

			file, err := os.Create(snapshotFile)
			if err != nil {
				qlog.Error("Failed to create final snapshot file: %v", err)
			} else {
				_, err = me.db.Backup(file, 0)

				closeErr := file.Close()
				if closeErr != nil {
					qlog.Warn("Error closing snapshot file: %v", closeErr)
				}

				if err != nil {
					qlog.Error("Failed to create final snapshot: %v", err)
					os.Remove(snapshotFile)
				} else {
					qlog.Info("Final snapshot created successfully: %s", snapshotFile)
				}
			}
		}
	}

	me.Close()
	me.isConnected = false
	me.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx})
}

// findLatestSnapshot returns the path to the latest snapshot file
func findLatestSnapshot(snapshotDir string) string {
	if snapshotDir == "" {
		return ""
	}

	files, err := filepath.Glob(filepath.Join(snapshotDir, "badger_snapshot_*.bak"))
	if err != nil || len(files) == 0 {
		return ""
	}

	// Sort files by name (which includes timestamp)
	sort.Strings(files)

	// Return the latest snapshot (last in sorted list)
	return files[len(files)-1]
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
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

	// Skip background tasks if explicitly disabled
	if me.config.DisableBackgroundTasks {
		return
	}

	// Create snapshot directory if needed and specified
	if me.config.SnapshotDirectory != "" {
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

	// Start snapshot task (for any DB with a snapshot directory)
	if me.config.SnapshotDirectory != "" && me.config.SnapshotInterval > 0 {
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
			if me.db == nil {
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
		err := me.db.Close()
		me.db = nil
		return err
	}
	return nil
}
