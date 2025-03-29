package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type NatsConnector struct {
	core NatsCore

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]
}

func NewConnector(core NatsCore) qdata.StoreConnector {
	connector := &NatsConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}

	core.Connected().Connect(connector.onConnected)
	core.Disconnected().Connect(connector.onDisconnected)

	return connector
}

func (me *NatsConnector) Connect(ctx context.Context) {
	if me.IsConnected() {
		return
	}

	me.core.Connect(ctx)
}

func (me *NatsConnector) Disconnect(ctx context.Context) {
	me.core.Disconnect(ctx)
}

func (me *NatsConnector) IsConnected() bool {
	return me.core.IsConnected()
}

func (me *NatsConnector) CheckConnection(ctx context.Context) bool {
	return me.core.CheckConnection(ctx)
}

func (me *NatsConnector) onConnected(args qdata.ConnectedArgs) {
	me.connected.Emit(args)
}

func (me *NatsConnector) onDisconnected(args qdata.DisconnectedArgs) {
	me.disconnected.Emit(args)
}

func (me *NatsConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

func (me *NatsConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}
