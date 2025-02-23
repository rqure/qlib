package store

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/store/postgres"
	"github.com/rqure/qlib/pkg/data/transformer"
)

type storeInternal interface {
	postgres.Core
}

type Store struct {
	storeInternal

	data.Connector
	data.EntityManager
	data.FieldOperator
	data.NotificationConsumer
	data.NotificationPublisher
	data.SchemaManager
	data.SnapshotManager

	transformer data.Transformer
}

func New() data.Store {
	core := postgres.NewCore(postgres.Config{})
	store := &Store{
		storeInternal:         core,
		Connector:             postgres.NewConnector(core),
		SchemaManager:         postgres.NewSchemaManager(core),
		EntityManager:         postgres.NewEntityManager(core),
		FieldOperator:         postgres.NewFieldOperator(core),
		SnapshotManager:       postgres.NewSnapshotManager(core),
		NotificationConsumer:  postgres.NewNotificationManager(core),
		NotificationPublisher: postgres.NewNotificationPublisher(core),
	}

	store.transformer = transformer.NewTransformer(store)
	store.EntityManager.(*postgres.EntityManager).SetFieldOperator(store.FieldOperator)
	store.EntityManager.(*postgres.EntityManager).SetSchemaManager(store.SchemaManager)

	store.NotificationPublisher.(*postgres.NotificationPublisher).SetEntityManager(store.EntityManager)
	store.NotificationPublisher.(*postgres.NotificationPublisher).SetFieldOperator(store.FieldOperator)

	store.NotificationConsumer.(*postgres.NotificationConsumer).SetTransformer(store.transformer)

	store.SchemaManager.(*postgres.SchemaManager).SetEntityManager(store.EntityManager)
	store.SchemaManager.(*postgres.SchemaManager).SetFieldOperator(store.FieldOperator)

	store.FieldOperator.(*postgres.FieldOperator).SetSchemaManager(store.SchemaManager)
	store.FieldOperator.(*postgres.FieldOperator).SetEntityManager(store.EntityManager)
	store.FieldOperator.(*postgres.FieldOperator).SetNotificationPublisher(store.NotificationPublisher)
	store.FieldOperator.(*postgres.FieldOperator).SetTransformer(store.transformer)

	store.SnapshotManager.(*postgres.SnapshotManager).SetSchemaManager(store.SchemaManager)
	store.SnapshotManager.(*postgres.SnapshotManager).SetEntityManager(store.EntityManager)
	store.SnapshotManager.(*postgres.SnapshotManager).SetFieldOperator(store.FieldOperator)

	return store
}
