package qpostgres

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresConfig struct {
	ConnectionString string
}

type PostgresCore interface {
	SetPool(pool *pgxpool.Pool)
	GetPool() *pgxpool.Pool
	SetConfig(config PostgresConfig)
	GetConfig() PostgresConfig
}

type postgresCore struct {
	pool   *pgxpool.Pool
	tx     pgx.Tx
	config PostgresConfig
}

func NewCore(config PostgresConfig) PostgresCore {
	return &postgresCore{config: config}
}

func (me *postgresCore) SetPool(pool *pgxpool.Pool) {
	me.pool = pool
}

func (me *postgresCore) GetPool() *pgxpool.Pool {
	return me.pool
}

func (me *postgresCore) SetConfig(config PostgresConfig) {
	me.config = config
}

func (me *postgresCore) GetConfig() PostgresConfig {
	return me.config
}
