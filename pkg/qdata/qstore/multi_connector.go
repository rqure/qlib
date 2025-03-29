package qstore

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type multiConnectorImpl struct {
	connectors []qdata.StoreConnector

	// Signals
	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]
}

type MultiConnector interface {
	qdata.StoreConnector
	AddConnector(connector qdata.StoreConnector)
}

func NewMultiConnector() MultiConnector {
	return &multiConnectorImpl{
		connectors:   make([]qdata.StoreConnector, 0),
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}
}

func (me *multiConnectorImpl) AddConnector(connector qdata.StoreConnector) {
	connector.Connected().Connect(me.onSubConnectionEstablished)
	connector.Disconnected().Connect(me.onSubConnectionLost)

	me.connectors = append(me.connectors, connector)
}

func (me *multiConnectorImpl) Connect(ctx context.Context) {
	for _, connector := range me.connectors {
		connector.Connect(ctx)
	}
}

func (me *multiConnectorImpl) Disconnect(ctx context.Context) {
	for _, connector := range me.connectors {
		connector.Disconnect(ctx)
	}
}

func (me *multiConnectorImpl) IsConnected() bool {
	for _, connector := range me.connectors {
		if !connector.IsConnected() {
			return false
		}
	}

	return len(me.connectors) > 0
}

func (me *multiConnectorImpl) CheckConnection(ctx context.Context) bool {
	for _, connector := range me.connectors {
		if !connector.CheckConnection(ctx) {
			return false
		}
	}

	return true
}

func (me *multiConnectorImpl) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

func (me *multiConnectorImpl) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}

func (me *multiConnectorImpl) onSubConnectionEstablished(args qdata.ConnectedArgs) {
	if !me.IsConnected() {
		return
	}

	// Emit the connected signal only if all connectors are connected
	me.connected.Emit(args)
}

func (me *multiConnectorImpl) onSubConnectionLost(args qdata.DisconnectedArgs) {
	me.disconnected.Emit(args)
}
