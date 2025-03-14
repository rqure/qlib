package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
	"github.com/rqure/qlib/pkg/qss/qsignal"
)

type Connector struct {
	core Core

	connected    qss.Signal
	disconnected qss.Signal
}

func NewConnector(core Core) qdata.Connector {
	connector := &Connector{
		core:         core,
		connected:    qsignal.New(),
		disconnected: qsignal.New(),
	}

	core.ReadyToConsume().Connect(connector.onReadyToConsume)
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

func (c *Connector) onReadyToConsume() {
	c.connected.Emit()
}

func (c *Connector) onDisconnected(err error) {
	c.disconnected.Emit(err)
}

func (c *Connector) Connected() qss.Signal {
	return c.connected
}

func (c *Connector) Disconnected() qss.Signal {
	return c.disconnected
}
