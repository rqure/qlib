package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
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

	healthCheckCtx    context.Context
	healthCheckCancel context.CancelFunc
	healthCheckMu     sync.Mutex

	// Use atomic operations for connection state
	isConnected atomic.Bool

	// Protect connection operations
	connMu sync.Mutex
}

func NewConnector(core Core) data.Connector {
	return &Connector{
		core:         core,
		connected:    signal.New(),
		disconnected: signal.New(),
	}
}

func (me *Connector) startHealthCheck() {
	me.healthCheckMu.Lock()
	defer me.healthCheckMu.Unlock()

	if me.healthCheckCtx != nil {
		return
	}

	me.healthCheckCtx, me.healthCheckCancel = context.WithCancel(context.Background())
	go me.healthCheckWorker(me.healthCheckCtx)
}

func (me *Connector) stopHealthCheck() {
	me.healthCheckMu.Lock()
	defer me.healthCheckMu.Unlock()

	if me.healthCheckCancel != nil {
		me.healthCheckCancel()
		me.healthCheckCtx = nil
		me.healthCheckCancel = nil
	}
}

func (me *Connector) setConnected(connected bool, err error) {
	wasConnected := me.isConnected.Swap(connected)
	if wasConnected != connected {
		if connected {
			me.connected.Emit()
		} else {
			me.disconnected.Emit(err)
		}
	}
}

func (me *Connector) closePool() {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}
	me.setConnected(false, nil)
}

func (me *Connector) healthCheckWorker(ctx context.Context) {
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

func (me *Connector) Connect(ctx context.Context) {
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
		log.Error("Failed to parse connection string: %v", err)
		me.setConnected(false, err)
		return
	}

	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		me.setConnected(true, nil)
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Error("Failed to create connection pool: %v", err)
		return
	}

	me.core.SetPool(pool)

	me.setConnected(true, nil)
	me.startHealthCheck()
}

func (me *Connector) Disconnect(ctx context.Context) {
	me.connMu.Lock()
	defer me.connMu.Unlock()

	me.stopHealthCheck()
	if me.core.GetPool() != nil {
		me.core.GetPool().Close()
		me.core.SetPool(nil)
	}
	me.setConnected(false, nil)
}

func (me *Connector) IsConnected(ctx context.Context) bool {
	return me.isConnected.Load()
}

func (me *Connector) Connected() signalslots.Signal {
	return me.connected
}

func (me *Connector) Disconnected() signalslots.Signal {
	return me.disconnected
}
