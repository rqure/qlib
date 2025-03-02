package data

import (
	"context"

	"github.com/rqure/qlib/pkg/auth"
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
	DeleteEntity(ctx context.Context, entityId string)
	FindEntities(ctx context.Context, entityType string) []string
	GetEntityTypes(ctx context.Context) []string
	EntityExists(ctx context.Context, entityId string) bool
}

type ModifiableEntityManager interface {
	SchemaManagerSetter
	FieldOperatorSetter
	EntityManager
}

type SchemaManager interface {
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

type FieldAuthorizer interface {
	AccessorId() string
	IsAuthorized(ctx context.Context, entityId, fieldName string) bool
}

type FieldReader interface {
	AuthorizedRead(context.Context, FieldAuthorizer, ...Request)
	Read(context.Context, ...Request)
}

type FieldWriter interface {
	AuthorizedWrite(context.Context, FieldAuthorizer, ...Request)
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
	Consumed() signalslots.Signal
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

type SessionProvider interface {
	Session(ctx context.Context) auth.Session
}

type Store interface {
	Connector
	SnapshotManager
	EntityManager
	SchemaManager
	FieldOperator
	NotificationConsumer
	NotificationPublisher
	SessionProvider
}
