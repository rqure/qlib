package qpostgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type PostgresConnector struct {
	core PostgresCore

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]

	isConnected bool
}

func NewConnector(core PostgresCore) qdata.StoreConnector {
	return &PostgresConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}
}

func (me *PostgresConnector) setConnected(ctx context.Context, connected bool, err error) {
	if connected == me.isConnected {
		return
	}

	me.isConnected = connected

	if connected {
		me.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
	} else {
		me.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx, Err: err})
	}
}

func (me *PostgresConnector) closePool() {
	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}
}

func (me *PostgresConnector) Connect(ctx context.Context) {
	if me.IsConnected() {
		return
	}

	me.closePool()

	config, err := pgxpool.ParseConfig(me.core.GetConfig().ConnectionString)
	if err != nil {
		qlog.Error("Failed to parse connection string: %v", err)
		me.setConnected(ctx, false, err)
		return
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		qlog.Error("Failed to create connection pool: %v", err)
		me.setConnected(ctx, false, err)
		return
	}

	me.core.SetPool(pool)
}

func (me *PostgresConnector) Disconnect(ctx context.Context) {
	me.closePool()
	me.setConnected(ctx, false, nil)
}

func (me *PostgresConnector) IsConnected() bool {
	return me.isConnected
}

func (me *PostgresConnector) CheckConnection(ctx context.Context) bool {
	pool := me.core.GetPool()

	if pool != nil {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			me.closePool()
			me.setConnected(ctx, false, err)
		} else {
			me.setConnected(ctx, true, nil)
		}
	}

	return me.IsConnected()
}

func (me *PostgresConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

func (me *PostgresConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}
