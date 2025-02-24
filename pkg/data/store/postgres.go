package store

import "github.com/rqure/qlib/pkg/data/store/postgres"

func PersistOverPostgres(address string) ConfigFn {
	return func(store *Store) {
		core := postgres.NewCore(postgres.Config{ConnectionString: address})

		store.MultiConnector.AddConnector(postgres.NewConnector(core))
		store.ModifiableSchemaManager = postgres.NewSchemaManager(core)
		store.ModifiableEntityManager = postgres.NewEntityManager(core)
		store.ModifiableFieldOperator = postgres.NewFieldOperator(core)
		store.ModifiableSnapshotManager = postgres.NewSnapshotManager(core)
	}
}
