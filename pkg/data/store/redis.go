package store

import "github.com/rqure/qlib/pkg/data/store/redis"

func CommunicateOverRedis(address string, password string) ConfigFn {
	return func(store *Store) {
		core := redis.NewCore(redis.Config{
			Address:  address,
			Password: password,
		})

		store.MultiConnector.AddConnector(redis.NewConnector(core))
		store.ModifiableSchemaManager = redis.NewSchemaManager(core)
		store.ModifiableEntityManager = redis.NewEntityManager(core)
		store.ModifiableFieldOperator = redis.NewFieldOperator(core)
		store.ModifiableSnapshotManager = redis.NewSnapshotManager(core)

		store.ModifiableNotificationConsumer = redis.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = redis.NewNotificationPublisher(core)
	}
}

func PersistOverRedis(address string, password string) ConfigFn {
	return func(store *Store) {
		core := redis.NewCore(redis.Config{
			Address:  address,
			Password: password,
		})

		store.MultiConnector.AddConnector(redis.NewConnector(core))
		store.ModifiableSchemaManager = redis.NewSchemaManager(core)
		store.ModifiableEntityManager = redis.NewEntityManager(core)
		store.ModifiableFieldOperator = redis.NewFieldOperator(core)
		store.ModifiableSnapshotManager = redis.NewSnapshotManager(core)
	}
}

func NotifyOverRedis(address string, password string) ConfigFn {
	return func(store *Store) {
		core := redis.NewCore(redis.Config{
			Address:  address,
			Password: password,
		})

		store.ModifiableNotificationConsumer = redis.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = redis.NewNotificationPublisher(core)
	}
}
