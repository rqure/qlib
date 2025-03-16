package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type Connector struct {
	core Core

	connected    qss.Signal[qss.VoidType]
	disconnected qss.Signal[error]
}

func NewConnector(core Core) qdata.Connector {
	connector := &Connector{
		core:         core,
		connected:    qss.New[qss.VoidType](),
		disconnected: qss.New[error](),
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

func (c *Connector) onConnected(qss.VoidType) {
	c.connected.Emit(qss.Void)
}

func (c *Connector) onDisconnected(err error) {
	c.disconnected.Emit(err)
}

func (c *Connector) Connected() qss.Signal[qss.VoidType] {
	return c.connected
}

func (c *Connector) Disconnected() qss.Signal[error] {
	return c.disconnected
}
