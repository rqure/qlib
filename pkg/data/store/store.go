package store

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/store/postgres"
	"github.com/rqure/qlib/pkg/data/transformer"
)

type Store struct {
	data.Connector
	data.ModifiableEntityManager
	data.ModifiableFieldOperator
	data.ModifiableNotificationConsumer
	data.ModifiableNotificationPublisher
	data.ModifiableSchemaManager
	data.ModifiableSnapshotManager

	transformer data.Transformer
}

type ConfigFn func(*Store)

func Connection(address string) ConfigFn {
	return func(store *Store) {
		core := postgres.NewCore(postgres.Config{ConnectionString: address})

		store.Connector = postgres.NewConnector(core)
		store.ModifiableSchemaManager = postgres.NewSchemaManager(core)
		store.ModifiableEntityManager = postgres.NewEntityManager(core)
		store.ModifiableFieldOperator = postgres.NewFieldOperator(core)
		store.ModifiableSnapshotManager = postgres.NewSnapshotManager(core)
		store.ModifiableNotificationConsumer = postgres.NewNotificationConsumer(core)
		store.ModifiableNotificationPublisher = postgres.NewNotificationPublisher(core)

		store.transformer = transformer.NewTransformer(store)
	}
}

func New(fn ...ConfigFn) data.Store {
	store := &Store{}

	for _, f := range fn {
		f(store)
	}

	store.ModifiableEntityManager.SetFieldOperator(store.ModifiableFieldOperator)
	store.ModifiableEntityManager.SetSchemaManager(store.ModifiableSchemaManager)

	store.ModifiableNotificationPublisher.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableNotificationPublisher.SetFieldOperator(store.ModifiableFieldOperator)

	store.ModifiableNotificationConsumer.SetTransformer(store.transformer)

	store.ModifiableSchemaManager.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableSchemaManager.SetFieldOperator(store.ModifiableFieldOperator)

	store.ModifiableFieldOperator.SetSchemaManager(store.ModifiableSchemaManager)
	store.ModifiableFieldOperator.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableFieldOperator.SetNotificationPublisher(store.ModifiableNotificationPublisher)
	store.ModifiableFieldOperator.SetTransformer(store.transformer)

	store.ModifiableSnapshotManager.SetSchemaManager(store.ModifiableSchemaManager)
	store.ModifiableSnapshotManager.SetEntityManager(store.ModifiableEntityManager)
	store.ModifiableSnapshotManager.SetFieldOperator(store.ModifiableFieldOperator)

	return store
}
