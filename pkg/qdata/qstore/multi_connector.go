package qstore

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type multiConnectorImpl struct {
	connectors []qdata.StoreConnector
	connMu     sync.RWMutex

	// Connection state tracking
	connectedCount atomic.Int32

	// Signals
	connected    qss.Signal[qss.VoidType]
	disconnected qss.Signal[error]
}

type MultiConnector interface {
	qdata.StoreConnector
	AddConnector(connector qdata.StoreConnector)
}

func NewMultiConnector() MultiConnector {
	return &multiConnectorImpl{
		connectors:   make([]qdata.StoreConnector, 0),
		connected:    qss.New[qss.VoidType](),
		disconnected: qss.New[error](),
	}
}

func (me *multiConnectorImpl) AddConnector(connector qdata.StoreConnector) {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	// Connect signals before adding to slice to avoid race conditions
	connector.Connected().Connect(func(qss.VoidType) {
		if me.onConnectorConnected() {
			me.connected.Emit(qss.Void)
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

func (me *multiConnectorImpl) Connected() qss.Signal[qss.VoidType] {
	return me.connected
}

func (me *multiConnectorImpl) Disconnected() qss.Signal[error] {
	return me.disconnected
}
