package qbadger

import (
	"context"
	"os"
	"path/filepath"
	"sort"

	"github.com/dgraph-io/badger/v4"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

// BadgerConnector manages the BadgerDB connection
type BadgerConnector struct {
	core BadgerCore

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]

	isConnected bool
}

// NewConnector creates a new BadgerDB connector
func NewConnector(core BadgerCore) qdata.StoreConnector {
	return &BadgerConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}
}

func (me *BadgerConnector) setConnected(ctx context.Context, connected bool, err error) {
	if connected == me.isConnected {
		return
	}

	me.isConnected = connected

	if connected {
		me.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
	} else {
		me.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx, Err: err})
	}
}

func (me *BadgerConnector) closeDB() {
	if me.core.GetDB() != nil {
		me.core.StopBackgroundTasks()
		_ = me.core.Close()
		me.core.SetDB(nil)
	}
}

// Connect establishes a connection to BadgerDB
func (me *BadgerConnector) Connect(ctx context.Context) {
	if me.IsConnected() {
		return
	}

	me.closeDB()

	config := me.core.GetConfig()
	var options badger.Options

	if config.InMemory {
		options = badger.DefaultOptions("").WithInMemory(true)
	} else {
		options = badger.DefaultOptions(config.Path)
		if config.ValueDir != "" {
			options = options.WithValueDir(config.ValueDir)
		}
	}

	// Configure ValueLogFileSize if specified
	if config.ValueSize > 0 {
		options = options.WithValueLogFileSize(config.ValueSize)
	}

	// Check for existing database or snapshots
	if !config.InMemory && config.Path != "" {
		// Try loading from latest snapshot if available and DB doesn't exist
		if !fileExists(config.Path) && config.SnapshotDirectory != "" {
			latestSnapshot := findLatestSnapshot(config.SnapshotDirectory)
			if latestSnapshot != "" {
				qlog.Info("Found database snapshot: %s", latestSnapshot)

				// Create directory for DB if it doesn't exist
				if err := os.MkdirAll(filepath.Dir(config.Path), 0755); err != nil {
					qlog.Error("Failed to create database directory: %v", err)
				} else {
					// Open the database from snapshot
					db, err := badger.Open(options)
					if err == nil {
						me.core.SetDB(db)

						// Open the snapshot file for reading
						file, err := os.Open(latestSnapshot)
						if err != nil {
							qlog.Error("Failed to open snapshot file: %v", err)
							_ = db.Close()
							me.core.SetDB(nil)
						} else {
							// Load from the snapshot file
							qlog.Info("Loading database from snapshot: %s", latestSnapshot)
							err = db.Load(file, 1) // 1 worker thread

							// Close the file after loading
							if closeErr := file.Close(); closeErr != nil {
								qlog.Warn("Error closing snapshot file: %v", closeErr)
							}

							if err != nil {
								qlog.Error("Failed to load database from snapshot: %v", err)
								_ = db.Close()
								me.core.SetDB(nil)
							} else {
								qlog.Info("Database loaded successfully from snapshot")
								me.setConnected(ctx, true, nil)
								me.core.StartBackgroundTasks(ctx)
								return
							}
						}
					}
				}
			}
		}
	}

	// Open the database normally
	db, err := badger.Open(options)
	if err != nil {
		qlog.Error("Failed to connect to BadgerDB: %v", err)
		me.setConnected(ctx, false, err)
		return
	}

	me.core.SetDB(db)
	me.setConnected(ctx, true, nil)

	// Start background tasks
	me.core.StartBackgroundTasks(ctx)
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

// Disconnect closes the BadgerDB connection
func (me *BadgerConnector) Disconnect(ctx context.Context) {
	me.closeDB()
	me.setConnected(ctx, false, nil)
}

// IsConnected returns the connection status
func (me *BadgerConnector) IsConnected() bool {
	return me.isConnected
}

// CheckConnection verifies the BadgerDB connection is still alive
func (me *BadgerConnector) CheckConnection(ctx context.Context) bool {
	db := me.core.GetDB()

	if db == nil {
		return false
	}

	// Try a simple operation to verify the connection
	err := db.View(func(txn *badger.Txn) error {
		// Just using NewIterator as a check if the DB is responsive
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		it.Close()
		return nil
	})

	if err != nil {
		me.closeDB()
		me.setConnected(ctx, false, err)
		return false
	}

	me.setConnected(ctx, true, nil)
	return true
}

// Connected returns the connected signal
func (me *BadgerConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

// Disconnected returns the disconnected signal
func (me *BadgerConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}
