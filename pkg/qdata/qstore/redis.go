package qstore

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qredis"
)

func PersistOverRedis(addr string, password string, db int, poolSize int) qdata.StoreOpts {
	return func(store *qdata.Store) {
		core := qredis.NewCore(qredis.RedisConfig{
			Addr:     addr,
			Password: password,
			DB:       db,
			PoolSize: poolSize,
		})

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qredis.NewConnector(core))
		} else {
			store.StoreConnector = qredis.NewConnector(core)
		}

		store.StoreInteractor = qredis.NewStoreInteractor(core)
	}
}
