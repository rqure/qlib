package qstore

import "github.com/rqure/qlib/pkg/qdata/qstore/qpostgres"

func PersistOverPostgres(address string) ConfigFn {
	return func(store *Store) {
		core := qpostgres.NewCore(qpostgres.PostgresConfig{ConnectionString: address})

		store.MultiConnector.AddConnector(qpostgres.NewConnector(core))
		store.ModifiableSchemaManager = qpostgres.NewSchemaManager(core)
		store.ModifiableEntityManager = qpostgres.NewEntityManager(core)
		store.ModifiableFieldOperator = qpostgres.NewFieldOperator(core)
		store.ModifiableSnapshotManager = qpostgres.NewSnapshotManager(core)
	}
}
