package qstore

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qpostgres"
)

func PersistOverPostgres(address string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		core := qpostgres.NewCore(qpostgres.PostgresConfig{ConnectionString: address})

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qpostgres.NewConnector(core))
		} else {
			store.StoreConnector = qpostgres.NewConnector(core)
		}

		store.StoreInteractor = qpostgres.NewStoreInteractor(core)
	}
}
