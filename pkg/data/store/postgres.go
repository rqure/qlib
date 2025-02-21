package store

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/store/postgres"
	"github.com/rqure/qlib/pkg/data/transformer"
)

type Postgres struct {
	postgres.Core
	data.Connector
	data.NotificationManager
	data.SchemaManager
	data.EntityManager
	data.FieldOperator
	data.SnapshotManager
	transformer data.Transformer
}

func NewPostgres(config postgres.Config) data.Store {
	core := postgres.NewCore()
	store := &Postgres{
		Core:                core,
		Connector:           postgres.NewConnector(core),
		NotificationManager: postgres.NewNotificationManager(core),
		SchemaManager:       postgres.NewSchemaManager(core),
		EntityManager:       postgres.NewEntityManager(core),
		FieldOperator:       postgres.NewFieldOperator(core),
		SnapshotManager:     postgres.NewSnapshotManager(core),
	}

	store.transformer = transformer.NewTransformer(store)
	store.EntityManager.(*postgres.EntityManager).SetFieldOperator(store.FieldOperator)
	store.EntityManager.(*postgres.EntityManager).SetSchemaManager(store.SchemaManager)

	store.NotificationManager.(*postgres.NotificationManager).SetEntityManager(store.EntityManager)
	store.NotificationManager.(*postgres.NotificationManager).SetFieldOperator(store.FieldOperator)
	store.NotificationManager.(*postgres.NotificationManager).SetTransformer(store.transformer)

	store.SchemaManager.(*postgres.SchemaManager).SetEntityManager(store.EntityManager)
	store.SchemaManager.(*postgres.SchemaManager).SetFieldOperator(store.FieldOperator)

	store.FieldOperator.(*postgres.FieldOperator).SetSchemaManager(store.SchemaManager)
	store.FieldOperator.(*postgres.FieldOperator).SetEntityManager(store.EntityManager)
	store.FieldOperator.(*postgres.FieldOperator).SetNotificationManager(store.NotificationManager)
	store.FieldOperator.(*postgres.FieldOperator).SetTransformer(store.transformer)

	store.SnapshotManager.(*postgres.SnapshotManager).SetSchemaManager(store.SchemaManager)
	store.SnapshotManager.(*postgres.SnapshotManager).SetEntityManager(store.EntityManager)
	store.SnapshotManager.(*postgres.SnapshotManager).SetFieldOperator(store.FieldOperator)

	return store
}
