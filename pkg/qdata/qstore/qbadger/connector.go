package qbadger

import (
	"context"

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

	// Open the database
	db, err := badger.Open(options)
	if err != nil {
		qlog.Error("Failed to connect to BadgerDB: %v", err)
		me.setConnected(ctx, false, err)
		return
	}

	me.core.SetDB(db)
	me.setConnected(ctx, true, nil)
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
