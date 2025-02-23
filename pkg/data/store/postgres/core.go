package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/log"
)

type Config struct {
	ConnectionString string
}

type Core interface {
	WithTx(ctx context.Context, fn func(context.Context, pgx.Tx))
	SetPool(pool *pgxpool.Pool)
	GetPool() *pgxpool.Pool
	SetConfig(config Config)
	GetConfig() Config
}

type coreInternal struct {
	pool   *pgxpool.Pool
	tx     pgx.Tx
	config Config
}

func NewCore(config Config) Core {
	return &coreInternal{config: config}
}

func (me *coreInternal) SetPool(pool *pgxpool.Pool) {
	me.pool = pool
}

func (me *coreInternal) GetPool() *pgxpool.Pool {
	return me.pool
}

func (me *coreInternal) SetConfig(config Config) {
	me.config = config
}

func (me *coreInternal) GetConfig() Config {
	return me.config
}

func (me *coreInternal) WithTx(ctx context.Context, fn func(context.Context, pgx.Tx)) {
	if me.tx == nil {
		tx, err := me.pool.Begin(ctx)
		if err != nil {
			log.Error("Failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		me.tx = tx
		fn(ctx, tx)
		me.tx = nil

		err = tx.Commit(ctx)
		if err != nil {
			log.Error("Failed to commit transaction: %v", err)
		}
	} else {
		fn(ctx, me.tx)
	}
}
