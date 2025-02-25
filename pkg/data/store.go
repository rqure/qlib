package data

import (
	"context"

	"github.com/rqure/qlib/pkg/signalslots"
)

type EntityManagerSetter interface {
	SetEntityManager(EntityManager)
}

type SchemaManagerSetter interface {
	SetSchemaManager(SchemaManager)
}

type FieldOperatorSetter interface {
	SetFieldOperator(FieldOperator)
}

type NotificationPublisherSetter interface {
	SetNotificationPublisher(NotificationPublisher)
}

type NotificationConsumerSetter interface {
	SetNotificationConsumer(NotificationConsumer)
}

type SnapshotManagerSetter interface {
	SetSnapshotManager(SnapshotManager)
}

type TransformerSetter interface {
	SetTransformer(Transformer)
}

type Connector interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected(context.Context) bool

	Connected() signalslots.Signal
	Disconnected() signalslots.Signal
}

type SnapshotManager interface {
	CreateSnapshot(context.Context) Snapshot
	RestoreSnapshot(context.Context, Snapshot)
}

type ModifiableSnapshotManager interface {
	SchemaManagerSetter
	EntityManagerSetter
	FieldOperatorSetter
	SnapshotManager
}

type EntityManager interface {
	CreateEntity(ctx context.Context, entityType, parentId, name string) string
	GetEntity(ctx context.Context, entityId string) Entity
	SetEntity(ctx context.Context, value Entity)
	DeleteEntity(ctx context.Context, entityId string)
	FindEntities(ctx context.Context, entityType string) []string
	GetEntityTypes(ctx context.Context) []string
}

type ModifiableEntityManager interface {
	SchemaManagerSetter
	FieldOperatorSetter
	EntityManager
}

type SchemaManager interface {
	EntityExists(ctx context.Context, entityId string) bool
	FieldExists(ctx context.Context, fieldName, entityType string) bool
	GetEntitySchema(ctx context.Context, entityType string) EntitySchema
	SetEntitySchema(context.Context, EntitySchema)
	GetFieldSchema(ctx context.Context, fieldName, entityType string) FieldSchema
	SetFieldSchema(ctx context.Context, entityType, fieldName string, schema FieldSchema)
}

type ModifiableSchemaManager interface {
	EntityManagerSetter
	FieldOperatorSetter
	SchemaManager
}

type FieldReader interface {
	Read(context.Context, ...Request)
}

type FieldWriter interface {
	Write(context.Context, ...Request)
}

type FieldOperator interface {
	FieldReader
	FieldWriter
}

type ModifiableFieldOperator interface {
	SchemaManagerSetter
	EntityManagerSetter
	NotificationPublisherSetter
	TransformerSetter
	FieldOperator
}

type NotificationConsumer interface {
	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(ctx context.Context, subscriptionId string)
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback)
	ProcessNotifications(context.Context)
}

type ModifiableNotificationConsumer interface {
	TransformerSetter
	NotificationConsumer
}

type NotificationPublisher interface {
	PublishNotifications(ctx context.Context, curr Request, prev Request)
}

type ModifiableNotificationPublisher interface {
	EntityManagerSetter
	FieldOperatorSetter
	NotificationPublisher
}

type IndirectionResolver interface {
	Resolve(ctx context.Context, entityId string, fields string) (string, string)
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
