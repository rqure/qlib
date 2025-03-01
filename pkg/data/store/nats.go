package store

import "github.com/rqure/qlib/pkg/data/store/nats"

func CommunicateOverNats(address string) ConfigFn {
	return func(store *Store) {
		core := nats.NewCore(nats.Config{Address: address})
		core.SetSessionProvider(store.SessionProvider)

		store.MultiConnector.AddConnector(nats.NewConnector(core))
		store.ModifiableSchemaManager = nats.NewSchemaManager(core)
		store.ModifiableEntityManager = nats.NewEntityManager(core)
		store.ModifiableFieldOperator = nats.NewFieldOperator(core)
		store.ModifiableSnapshotManager = nats.NewSnapshotManager(core)

		store.ModifiableNotificationConsumer = nats.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = nats.NewNotificationPublisher(core)
	}
}

func PersistOverNats(address string) ConfigFn {
	return func(store *Store) {
		core := nats.NewCore(nats.Config{Address: address})

		store.MultiConnector.AddConnector(nats.NewConnector(core))
		store.ModifiableSchemaManager = nats.NewSchemaManager(core)
		store.ModifiableEntityManager = nats.NewEntityManager(core)
		store.ModifiableFieldOperator = nats.NewFieldOperator(core)
		store.ModifiableSnapshotManager = nats.NewSnapshotManager(core)
	}
}

func NotifyOverNats(address string) ConfigFn {
	return func(store *Store) {
		core := nats.NewCore(nats.Config{Address: address})

		store.ModifiableNotificationConsumer = nats.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = nats.NewNotificationPublisher(core)
	}
}
