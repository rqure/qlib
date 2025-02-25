package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

type Connector struct {
	core Core

	connected    signalslots.Signal
	disconnected signalslots.Signal
}

func NewConnector(core Core) data.Connector {
	return &Connector{
		core:         core,
		connected:    signal.New(),
		disconnected: signal.New(),
	}
}

func (me *Connector) Connect(ctx context.Context) {
	if me.IsConnected(ctx) {
		return
	}

	me.Disconnect(ctx)

	config, err := pgxpool.ParseConfig(me.core.GetConfig().ConnectionString)
	if err != nil {
		log.Error("Failed to parse connection string: %v", err)
		return
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Error("Failed to create connection pool: %v", err)
		return
	}

	me.core.SetPool(pool)
}

func (me *Connector) Disconnect(ctx context.Context) {
	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}
}

func (me *Connector) IsConnected(ctx context.Context) bool {
	if me.core.GetPool() == nil {
		return false
	}
	return me.core.GetPool().Ping(ctx) == nil
}

func (me *Connector) Connected() signalslots.Signal {
	return me.connected
}

func (me *Connector) Disconnected() signalslots.Signal {
	return me.disconnected
}
