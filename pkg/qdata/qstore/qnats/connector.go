package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type NatsConnector struct {
	core NatsCore

	connected    qss.Signal[qss.VoidType]
	disconnected qss.Signal[error]
}

func NewConnector(core NatsCore) qdata.StoreConnector {
	connector := &NatsConnector{
		core:         core,
		connected:    qss.New[qss.VoidType](),
		disconnected: qss.New[error](),
	}

	core.Connected().Connect(connector.onConnected)
	core.Disconnected().Connect(connector.onDisconnected)

	return connector
}

func (c *NatsConnector) Connect(ctx context.Context) {
	if c.IsConnected(ctx) {
		return
	}

	c.core.Connect(ctx)
}

func (c *NatsConnector) Disconnect(ctx context.Context) {
	c.core.Disconnect(ctx)
}

func (c *NatsConnector) IsConnected(ctx context.Context) bool {
	return c.core.IsConnected(ctx)
}

func (c *NatsConnector) onConnected(qss.VoidType) {
	c.connected.Emit(qss.Void)
}

func (c *NatsConnector) onDisconnected(err error) {
	c.disconnected.Emit(err)
}

func (c *NatsConnector) Connected() qss.Signal[qss.VoidType] {
	return c.connected
}

func (c *NatsConnector) Disconnected() qss.Signal[error] {
	return c.disconnected
}
