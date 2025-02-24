package store

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/store/nats"
	"github.com/rqure/qlib/pkg/data/store/postgres"
	"github.com/rqure/qlib/pkg/data/store/redis"
	"github.com/rqure/qlib/pkg/data/store/web"
	"github.com/rqure/qlib/pkg/data/transformer"
)

type Store struct {
	MultiConnector

	data.ModifiableEntityManager
	data.ModifiableFieldOperator
	data.ModifiableNotificationConsumer
	data.ModifiableNotificationPublisher
	data.ModifiableSchemaManager
	data.ModifiableSnapshotManager

	data.Transformer
}

type ConfigFn func(*Store)

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

func New(fn ...ConfigFn) data.Store {
	store := &Store{}

	store.MultiConnector = NewMultiConnector()
	store.Transformer = transformer.NewTransformer(store)

	for _, f := range fn {
		f(store)
	}

	store.ModifiableEntityManager.SetFieldOperator(store.ModifiableFieldOperator)
	store.ModifiableEntityManager.SetSchemaManager(store.ModifiableSchemaManager)

	store.ModifiableNotificationPublisher.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableNotificationPublisher.SetFieldOperator(store.ModifiableFieldOperator)

	store.ModifiableNotificationConsumer.SetTransformer(store.Transformer)

	store.ModifiableSchemaManager.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableSchemaManager.SetFieldOperator(store.ModifiableFieldOperator)

	store.ModifiableFieldOperator.SetSchemaManager(store.ModifiableSchemaManager)
	store.ModifiableFieldOperator.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableFieldOperator.SetNotificationPublisher(store.ModifiableNotificationPublisher)
	store.ModifiableFieldOperator.SetTransformer(store.Transformer)

	store.ModifiableSnapshotManager.SetSchemaManager(store.ModifiableSchemaManager)
	store.ModifiableSnapshotManager.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableSnapshotManager.SetFieldOperator(store.ModifiableFieldOperator)

	return store
}
