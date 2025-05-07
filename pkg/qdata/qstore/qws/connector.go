package qws

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

// WebSocketConnector implements the StoreConnector interface for WebSocket connections
type WebSocketConnector struct {
	core         WebSocketCore
	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]
}

// NewConnector creates a new WebSocketConnector instance
func NewConnector(core WebSocketCore) qdata.StoreConnector {
	connector := &WebSocketConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}

	// Subscribe to connection events
	core.Connected().Connect(connector.onConnected)
	core.Disconnected().Connect(connector.onDisconnected)

	return connector
}

// Connect establishes a WebSocket connection
func (wc *WebSocketConnector) Connect(ctx context.Context) {
	if wc.IsConnected() {
		return
	}

	wc.core.Connect(ctx)
}

// Disconnect closes the WebSocket connection
func (wc *WebSocketConnector) Disconnect(ctx context.Context) {
	wc.core.Disconnect(ctx)
}

// IsConnected returns true if the WebSocket is connected
func (wc *WebSocketConnector) IsConnected() bool {
	return wc.core.IsConnected()
}

// CheckConnection tests if the WebSocket connection is alive
func (wc *WebSocketConnector) CheckConnection(ctx context.Context) bool {
	return wc.core.CheckConnection(ctx)
}

// Connected returns the signal that is emitted when the connection is established
func (wc *WebSocketConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return wc.connected
}

// Disconnected returns the signal that is emitted when the connection is lost
func (wc *WebSocketConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return wc.disconnected
}

// onConnected handles the connected event from the core
func (wc *WebSocketConnector) onConnected(args qdata.ConnectedArgs) {
	wc.connected.Emit(args)
}

// onDisconnected handles the disconnected event from the core
func (wc *WebSocketConnector) onDisconnected(args qdata.DisconnectedArgs) {
	wc.disconnected.Emit(args)
}
