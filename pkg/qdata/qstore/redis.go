package qstore

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qredis"
)

// Optionally pass go-cache options (expiration, cleanupInterval) to enable in-memory caching.
func PersistOverRedis(addr string, password string, db int, poolSize int, cacheOpts ...interface{}) qdata.StoreOpts {
	return func(store *qdata.Store) {
		core := qredis.NewCore(qredis.RedisConfig{
			Addr:     addr,
			Password: password,
			DB:       db,
			PoolSize: poolSize,
		})

		var interactor qdata.StoreInteractor
		if len(cacheOpts) == 2 {
			if exp, ok1 := cacheOpts[0].(time.Duration); ok1 {
				if cleanup, ok2 := cacheOpts[1].(time.Duration); ok2 {
					interactor = qredis.NewStoreInteractor(core, qredis.WithGoCache(exp, cleanup))
				}
			}
		}
		if interactor == nil {
			interactor = qredis.NewStoreInteractor(core)
		}

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qredis.NewConnector(core))
		} else {
			store.StoreConnector = qredis.NewConnector(core)
		}

		store.StoreInteractor = interactor
	}
}
