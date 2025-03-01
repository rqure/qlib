package store

import (
	"github.com/rqure/qlib/pkg/data"
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

	data.SessionProvider
}

type ConfigFn func(*Store)

func New(fn ...ConfigFn) data.Store {
	store := &Store{}

	store.MultiConnector = NewMultiConnector()
	store.Transformer = transformer.NewTransformer(store)
	store.SessionProvider = NewSessionProvider()

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
