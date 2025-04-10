package qpostgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rqure/qlib/pkg/qlog"
)

type PostgresConfig struct {
	ConnectionString string
}

type PostgresCore interface {
	WithTx(ctx context.Context, fn func(context.Context, pgx.Tx))
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

func (me *postgresCore) WithTx(ctx context.Context, fn func(context.Context, pgx.Tx)) {
	if me.pool == nil {
		qlog.Error("Postgres pool is not initialized")
		return
	}

	if me.tx == nil {
		tx, err := me.pool.Begin(ctx)
		if err != nil {
			qlog.Error("Failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		me.tx = tx
		fn(ctx, tx)
		me.tx = nil

		err = tx.Commit(ctx)
		if err != nil {
			qlog.Error("Failed to commit transaction: %v", err)
		}
	} else {
		fn(ctx, me.tx)
	}
}

const defaultBatchSize = 10000

func BatchedQuery[T any](c PostgresCore, ctx context.Context,
	query string,
	args []any,
	batchSize int,
	scan func(rows pgx.Rows, cursorId *int64) (T, error),
	process func(batch []T) error) error {

	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	var lastCursorId int64
	for {
		finalQuery := query
		finalArgs := make([]any, len(args))
		copy(finalArgs, args)
		if lastCursorId > 0 {
			finalQuery = fmt.Sprintf("%s AND cursor_id > $%d", query, len(args)+1)
			finalArgs = append(finalArgs, lastCursorId)
		}
		finalQuery = fmt.Sprintf("%s ORDER BY cursor_id LIMIT %d", finalQuery, batchSize)

		rows, err := c.GetPool().Query(ctx, finalQuery, finalArgs...)
		if err != nil {
			if rows != nil {
				rows.Close()
			}
			return fmt.Errorf("query failed: %w", err)
		}

		// Store batch results
		batch := make([]T, 0, batchSize)

		for rows.Next() {
			var cursorId int64
			item, err := scan(rows, &cursorId)
			if err != nil {
				rows.Close()
				return err
			}

			lastCursorId = cursorId

			batch = append(batch, item)
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return err
		}

		// Process the batch after rows are closed
		if len(batch) > 0 {
			if err := process(batch); err != nil {
				return err
			}
		}

		if len(batch) < batchSize {
			break
		}
	}

	return nil
}
