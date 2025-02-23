package store

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
)

type multiConnectorImpl struct {
	connectors []data.Connector
}

type MultiConnector interface {
	data.Connector
	AddConnector(connector data.Connector)
}

func NewMultiConnector() MultiConnector {
	return &multiConnectorImpl{
		connectors: make([]data.Connector, 0),
	}
}

func (me *multiConnectorImpl) AddConnector(connector data.Connector) {
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

func (me *multiConnectorImpl) IsConnected(ctx context.Context) bool {
	for _, connector := range me.connectors {
		if !connector.IsConnected(ctx) {
			return false
		}
	}

	return true
}
