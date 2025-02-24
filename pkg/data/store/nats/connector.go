package nats

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
)

type Connector struct {
	core Core
}

func NewConnector(core Core) data.Connector {
	return &Connector{core: core}
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
