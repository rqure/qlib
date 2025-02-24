package store

import "github.com/rqure/qlib/pkg/data/store/web"

func CommunicateOverWeb(address string) ConfigFn {
	return func(store *Store) {
		core := web.NewCore(web.Config{Address: address})

		store.MultiConnector.AddConnector(web.NewConnector(core))
		store.ModifiableSchemaManager = web.NewSchemaManager(core)
		store.ModifiableEntityManager = web.NewEntityManager(core)
		store.ModifiableFieldOperator = web.NewFieldOperator(core)
		store.ModifiableSnapshotManager = web.NewSnapshotManager(core)

		store.ModifiableNotificationConsumer = web.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = web.NewNotificationPublisher(core)
	}
}

func PersistOverWeb(address string) ConfigFn {
	return func(store *Store) {
		core := web.NewCore(web.Config{Address: address})

		store.MultiConnector.AddConnector(web.NewConnector(core))
		store.ModifiableSchemaManager = web.NewSchemaManager(core)
		store.ModifiableEntityManager = web.NewEntityManager(core)
		store.ModifiableFieldOperator = web.NewFieldOperator(core)
		store.ModifiableSnapshotManager = web.NewSnapshotManager(core)
	}
}

func NotifyOverWeb(address string) ConfigFn {
	return func(store *Store) {
		core := web.NewCore(web.Config{Address: address})

		store.ModifiableNotificationConsumer = web.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = web.NewNotificationPublisher(core)
	}
}
