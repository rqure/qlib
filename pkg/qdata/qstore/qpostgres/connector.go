package qpostgres

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type PostgresConnector struct {
	core PostgresCore

	connected    qss.Signal[qss.VoidType]
	disconnected qss.Signal[error]

	healthCheckCtx    context.Context
	healthCheckCancel context.CancelFunc
	healthCheckMu     sync.Mutex

	// Use atomic operations for connection state
	isConnected atomic.Bool

	// Protect connection operations
	connMu sync.Mutex
}

func NewConnector(core PostgresCore) qdata.StoreConnector {
	return &PostgresConnector{
		core:         core,
		connected:    qss.New[qss.VoidType](),
		disconnected: qss.New[error](),
	}
}

func (me *PostgresConnector) startHealthCheck() {
	me.healthCheckMu.Lock()
	defer me.healthCheckMu.Unlock()

	if me.healthCheckCtx != nil {
		return
	}

	me.healthCheckCtx, me.healthCheckCancel = context.WithCancel(context.Background())
	go me.healthCheckWorker(me.healthCheckCtx)
}

func (me *PostgresConnector) stopHealthCheck() {
	me.healthCheckMu.Lock()
	defer me.healthCheckMu.Unlock()

	if me.healthCheckCancel != nil {
		me.healthCheckCancel()
		me.healthCheckCtx = nil
		me.healthCheckCancel = nil
	}
}

func (me *PostgresConnector) setConnected(connected bool, err error) {
	wasConnected := me.isConnected.Swap(connected)
	if wasConnected != connected {
		if connected {
			me.connected.Emit(qss.Void)
		} else {
			me.disconnected.Emit(err)
		}
	}
}

func (me *PostgresConnector) closePool() {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}
	me.setConnected(false, nil)
}

func (me *PostgresConnector) healthCheckWorker(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			me.connMu.Lock()
			pool := me.core.GetPool()
			me.connMu.Unlock()

			if pool != nil {
				ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
				defer cancel()

				if err := pool.Ping(ctx); err != nil {
					me.stopHealthCheck()
					me.closePool()
					me.setConnected(false, err)
					return
				}
				me.setConnected(true, nil)
			}
		}
	}
}

func (me *PostgresConnector) Connect(ctx context.Context) {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	if me.IsConnected(ctx) {
		return
	}

	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}

	config, err := pgxpool.ParseConfig(me.core.GetConfig().ConnectionString)
	if err != nil {
		qlog.Error("Failed to parse connection string: %v", err)
		me.setConnected(false, err)
		return
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		qlog.Error("Failed to create connection pool: %v", err)
		me.setConnected(false, err)
		return
	}

	me.core.SetPool(pool)

	me.startHealthCheck()
}

func (me *PostgresConnector) Disconnect(ctx context.Context) {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	me.stopHealthCheck()
	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}
	me.setConnected(false, nil)
}

func (me *PostgresConnector) IsConnected(ctx context.Context) bool {
	return me.isConnected.Load()
}

func (me *PostgresConnector) Connected() qss.Signal[qss.VoidType] {
	return me.connected
}

func (me *PostgresConnector) Disconnected() qss.Signal[error] {
	return me.disconnected
}
