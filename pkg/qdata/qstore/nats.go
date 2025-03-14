package qstore

import "github.com/rqure/qlib/pkg/qdata/qstore/qnats"

func CommunicateOverNats(address string) ConfigFn {
	return func(store *Store) {
		core := qnats.NewCore(qnats.Config{Address: address})
		core.SetAuthProvider(store.AuthProvider)

		store.MultiConnector.AddConnector(qnats.NewConnector(core))
		store.ModifiableSchemaManager = qnats.NewSchemaManager(core)
		store.ModifiableEntityManager = qnats.NewEntityManager(core)
		store.ModifiableFieldOperator = qnats.NewFieldOperator(core)
		store.ModifiableSnapshotManager = qnats.NewSnapshotManager(core)

		store.ModifiableNotificationConsumer = qnats.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = qnats.NewNotificationPublisher(core)
	}
}

func PersistOverNats(address string) ConfigFn {
	return func(store *Store) {
		core := qnats.NewCore(qnats.Config{Address: address})
		core.SetAuthProvider(store.AuthProvider)

		store.MultiConnector.AddConnector(qnats.NewConnector(core))
		store.ModifiableSchemaManager = qnats.NewSchemaManager(core)
		store.ModifiableEntityManager = qnats.NewEntityManager(core)
		store.ModifiableFieldOperator = qnats.NewFieldOperator(core)
		store.ModifiableSnapshotManager = qnats.NewSnapshotManager(core)
	}
}

func NotifyOverNats(address string) ConfigFn {
	return func(store *Store) {
		core := qnats.NewCore(qnats.Config{Address: address})
		core.SetAuthProvider(store.AuthProvider)

		store.MultiConnector.AddConnector(qnats.NewConnector(core))
		store.ModifiableNotificationConsumer = qnats.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = qnats.NewNotificationPublisher(core)
	}
}
