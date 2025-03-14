package qstore

import (
	"github.com/rqure/qlib/pkg/qdata"
)

type Store struct {
	MultiConnector

	qdata.ModifiableEntityManager
	qdata.ModifiableFieldOperator
	qdata.ModifiableNotificationConsumer
	qdata.ModifiableNotificationPublisher
	qdata.ModifiableSchemaManager
	qdata.ModifiableSnapshotManager

	qdata.AuthProvider
}

type ConfigFn func(*Store)

func New(fn ...ConfigFn) qdata.Store {
	store := &Store{}

	store.MultiConnector = NewMultiConnector()
	store.AuthProvider = NewAuthProvider()

	for _, f := range fn {
		f(store)
	}

	if store.ModifiableEntityManager != nil {
		store.ModifiableEntityManager.SetFieldOperator(store.ModifiableFieldOperator)
		store.ModifiableEntityManager.SetSchemaManager(store.ModifiableSchemaManager)
	}

	if store.ModifiableNotificationPublisher != nil {
		store.ModifiableNotificationPublisher.SetEntityManager(store.ModifiableEntityManager)
		store.ModifiableNotificationPublisher.SetFieldOperator(store.ModifiableFieldOperator)
	}

	if store.ModifiableSchemaManager != nil {
		store.ModifiableSchemaManager.SetEntityManager(store.ModifiableEntityManager)
		store.ModifiableSchemaManager.SetFieldOperator(store.ModifiableFieldOperator)
	}

	if store.ModifiableFieldOperator != nil {
		store.ModifiableFieldOperator.SetSchemaManager(store.ModifiableSchemaManager)
		store.ModifiableFieldOperator.SetEntityManager(store.ModifiableEntityManager)
		store.ModifiableFieldOperator.SetNotificationPublisher(store.ModifiableNotificationPublisher)
	}

	if store.ModifiableSnapshotManager != nil {
		store.ModifiableSnapshotManager.SetSchemaManager(store.ModifiableSchemaManager)
		store.ModifiableSnapshotManager.SetEntityManager(store.ModifiableEntityManager)
		store.ModifiableSnapshotManager.SetFieldOperator(store.ModifiableFieldOperator)
	}

	return store
}
