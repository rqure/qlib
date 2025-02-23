package data

import (
	"context"
)

type Connector interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected(context.Context) bool
}

type SnapshotManager interface {
	CreateSnapshot(context.Context) Snapshot
	RestoreSnapshot(context.Context, Snapshot)
}

type EntityManager interface {
	CreateEntity(ctx context.Context, entityType, parentId, name string)
	GetEntity(ctx context.Context, entityId string) Entity
	SetEntity(ctx context.Context, value Entity)
	DeleteEntity(ctx context.Context, entityId string)
	FindEntities(ctx context.Context, entityType string) []string
	GetEntityTypes(ctx context.Context) []string
}

type SchemaManager interface {
	EntityExists(ctx context.Context, entityId string) bool
	FieldExists(ctx context.Context, fieldName, entityType string) bool
	GetEntitySchema(ctx context.Context, entityType string) EntitySchema
	SetEntitySchema(context.Context, EntitySchema)
	GetFieldSchema(ctx context.Context, fieldName, entityType string) FieldSchema
	SetFieldSchema(ctx context.Context, entityType, fieldName string, schema FieldSchema)
}

type FieldOperator interface {
	Read(context.Context, ...Request)
	Write(context.Context, ...Request)
}

type NotificationConsumer interface {
	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(ctx context.Context, subscriptionId string)
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback)
	ProcessNotifications(context.Context)
}

type NotificationPublisher interface {
	TriggerNotifications(ctx context.Context, curr Request, prev Request)
}

type Store interface {
	Connector
	SnapshotManager
	EntityManager
	SchemaManager
	FieldOperator
	NotificationConsumer
	NotificationPublisher
}
