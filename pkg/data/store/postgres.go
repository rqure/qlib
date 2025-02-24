package store

import "github.com/rqure/qlib/pkg/data/store/postgres"

func CommunicateOverPostgres(address string) ConfigFn {
	return func(store *Store) {
		core := postgres.NewCore(postgres.Config{ConnectionString: address})

		store.MultiConnector.AddConnector(postgres.NewConnector(core))
		store.ModifiableSchemaManager = postgres.NewSchemaManager(core)
		store.ModifiableEntityManager = postgres.NewEntityManager(core)
		store.ModifiableFieldOperator = postgres.NewFieldOperator(core)
		store.ModifiableSnapshotManager = postgres.NewSnapshotManager(core)

		store.ModifiableNotificationConsumer = postgres.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = postgres.NewNotificationPublisher(core)
	}
}

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

func NotifyOverPostgres(address string) ConfigFn {
	return func(store *Store) {
		core := postgres.NewCore(postgres.Config{ConnectionString: address})

		store.ModifiableNotificationConsumer = postgres.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = postgres.NewNotificationPublisher(core)
	}
}
