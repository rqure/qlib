package qmap

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

// MapConnector implements StoreConnector interface for map-based storage
type MapConnector struct {
	core MapCore

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]
}

// NewConnector creates a new map store connector
func NewConnector(core MapCore) qdata.StoreConnector {
	connector := &MapConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}

	// Subscribe to connection events from the core
	core.Connected().Connect(connector.onConnected)
	core.Disconnected().Connect(connector.onDisconnected)

	return connector
}

// Connect establishes a connection to the map store
func (c *MapConnector) Connect(ctx context.Context) {
	c.core.Connect(ctx)
}

// Disconnect closes the connection to the map store
func (c *MapConnector) Disconnect(ctx context.Context) {
	c.core.Disconnect(ctx)
}

// IsConnected returns whether the store is connected
func (c *MapConnector) IsConnected() bool {
	return c.core.IsConnected()
}

// Connected returns the connected signal
func (c *MapConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return c.connected
}

// Disconnected returns the disconnected signal
func (c *MapConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return c.disconnected
}

// onConnected handles connected events
func (c *MapConnector) onConnected(args qdata.ConnectedArgs) {
	c.connected.Emit(args)
}

// onDisconnected handles disconnected events
func (c *MapConnector) onDisconnected(args qdata.DisconnectedArgs) {
	c.disconnected.Emit(args)
}
