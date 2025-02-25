package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

type Connector struct {
	core Core

	connected    signalslots.Signal
	disconnected signalslots.Signal
}

func NewConnector(core Core) data.Connector {
	connector := &Connector{
		core:         core,
		connected:    signal.New(),
		disconnected: signal.New(),
	}

	core.Connected().Connect(connector.onConnected)
	core.Disconnected().Connect(connector.onDisconnected)

	return connector
}

func (c *Connector) Connect(ctx context.Context) {
	if c.IsConnected(ctx) {
		return
	}

	c.core.Connect(ctx)
}

func (c *Connector) Disconnect(ctx context.Context) {
	c.core.Disconnect(ctx)
}

func (c *Connector) IsConnected(ctx context.Context) bool {
	return c.core.IsConnected(ctx)
}

func (c *Connector) onConnected() {
	c.connected.Emit()
}

func (c *Connector) onDisconnected(err error) {
	c.disconnected.Emit(err)
}

func (c *Connector) Connected() signalslots.Signal {
	return c.connected
}

func (c *Connector) Disconnected() signalslots.Signal {
	return c.disconnected
}
