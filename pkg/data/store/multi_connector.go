package store

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

type multiConnectorImpl struct {
	connectors []data.Connector
	connMu     sync.RWMutex

	// Connection state tracking
	connectedCount atomic.Int32

	// Signals
	connected    signalslots.Signal
	disconnected signalslots.Signal
}

type MultiConnector interface {
	data.Connector
	AddConnector(connector data.Connector)
}

func NewMultiConnector() MultiConnector {
	return &multiConnectorImpl{
		connectors:   make([]data.Connector, 0),
		connected:    signal.New(),
		disconnected: signal.New(),
	}
}

func (me *multiConnectorImpl) AddConnector(connector data.Connector) {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	// Connect signals before adding to slice to avoid race conditions
	connector.Connected().Connect(func() {
		if me.onConnectorConnected() {
			me.connected.Emit()
		}
	})

	connector.Disconnected().Connect(func(err error) {
		me.onConnectorDisconnected()
		me.disconnected.Emit(err)
	})

	me.connectors = append(me.connectors, connector)
}

func (me *multiConnectorImpl) onConnectorConnected() bool {
	newCount := me.connectedCount.Add(1)
	me.connMu.RLock()
	allConnected := newCount == int32(len(me.connectors))
	me.connMu.RUnlock()
	return allConnected
}

func (me *multiConnectorImpl) onConnectorDisconnected() {
	me.connectedCount.Add(-1)
}

func (me *multiConnectorImpl) Connect(ctx context.Context) {
	me.connMu.RLock()
	defer me.connMu.RUnlock()

	for _, connector := range me.connectors {
		connector.Connect(ctx)
	}
}

func (me *multiConnectorImpl) Disconnect(ctx context.Context) {
	me.connMu.RLock()
	defer me.connMu.RUnlock()

	for _, connector := range me.connectors {
		connector.Disconnect(ctx)
	}
}

func (me *multiConnectorImpl) IsConnected(ctx context.Context) bool {
	me.connMu.RLock()
	defer me.connMu.RUnlock()

	for _, connector := range me.connectors {
		if !connector.IsConnected(ctx) {
			return false
		}
	}

	return len(me.connectors) > 0
}

func (me *multiConnectorImpl) Connected() signalslots.Signal {
	return me.connected
}

func (me *multiConnectorImpl) Disconnected() signalslots.Signal {
	return me.disconnected
}
