package qbadger

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

// BadgerConnector manages the BadgerDB connection
type BadgerConnector struct {
	core BadgerCore

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]
}

// NewConnector creates a new BadgerDB connector
func NewConnector(core BadgerCore) qdata.StoreConnector {
	connector := &BadgerConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}

	// Subscribe to connection events from the core
	core.Connected().Connect(connector.onConnected)
	core.Disconnected().Connect(connector.onDisconnected)

	return connector
}

// Connect establishes a connection to BadgerDB
func (me *BadgerConnector) Connect(ctx context.Context) {
	if me.IsConnected() {
		return
	}

	me.core.Connect(ctx)
}

// Disconnect closes the BadgerDB connection
func (me *BadgerConnector) Disconnect(ctx context.Context) {
	me.core.Disconnect(ctx)
}

// IsConnected returns the connection status
func (me *BadgerConnector) IsConnected() bool {
	return me.core.IsConnected()
}

// Connected returns the connected signal
func (me *BadgerConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

// Disconnected returns the disconnected signal
func (me *BadgerConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}

// onConnected handles the connected event from the core
func (me *BadgerConnector) onConnected(args qdata.ConnectedArgs) {
	me.connected.Emit(args)
}

// onDisconnected handles the disconnected event from the core
func (me *BadgerConnector) onDisconnected(args qdata.DisconnectedArgs) {
	me.disconnected.Emit(args)
}
