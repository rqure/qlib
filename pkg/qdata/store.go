package qdata

import (
	"context"

	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qss"
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

type Connector interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected(context.Context) bool

	Connected() qss.Signal[qss.VoidType]
	Disconnected() qss.Signal[error]
}

type SnapshotManager interface {
	CreateSnapshot(context.Context) Snapshot
	RestoreSnapshot(context.Context, Snapshot)
	InitializeIfRequired(context.Context)
}

type ModifiableSnapshotManager interface {
	SchemaManagerSetter
	EntityManagerSetter
	FieldOperatorSetter
	SnapshotManager
}

type FindEntitiesPaginatedResult interface {
	Next(context.Context) bool
	Value() string
	Error() error
	TotalCount() int
}

type EntityManager interface {
	CreateEntity(ctx context.Context, entityType, parentId, name string) string
	GetEntity(ctx context.Context, entityId string) Entity
	DeleteEntity(ctx context.Context, entityId string)
	Find(sql string) Query
	FindEntities(ctx context.Context, entityType string) []string
	FindEntitiesPaginated(ctx context.Context, entityType string, page, pageSize int) FindEntitiesPaginatedResult
	GetEntityTypes(ctx context.Context) []string
	EntityExists(ctx context.Context, entityId string) bool
}

type ModifiableEntityManager interface {
	SchemaManagerSetter
	FieldOperatorSetter
	EntityManager
}

type SchemaManager interface {
	FieldExists(ctx context.Context, entityType, fieldName string) bool
	GetEntitySchema(ctx context.Context, entityType string) ModifiableEntitySchema
	SetEntitySchema(context.Context, EntitySchema)
	GetFieldSchema(ctx context.Context, entityType, fieldName string) FieldSchema
	SetFieldSchema(ctx context.Context, entityType, fieldName string, schema FieldSchema)
}

type ModifiableSchemaManager interface {
	EntityManagerSetter
	FieldOperatorSetter
	SchemaManager
}

type FieldAuthorizerKeyType string

const FieldAuthorizerKey FieldAuthorizerKeyType = "Authorizer"

type FieldAuthorizer interface {
	AccessorId() string
	IsAuthorized(ctx context.Context, entityId, fieldName string, forWrite bool) bool
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
	FieldOperator
}

type NotificationConsumer interface {
	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(ctx context.Context, subscriptionId string)
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback)
	Consumed() qss.Signal[func(context.Context)]
}

type ModifiableNotificationConsumer interface {
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

type AuthProvider interface {
	AuthClient(ctx context.Context) qauth.Client
}

type Store interface {
	Connector
	EntityManager
	FieldOperator
	NotificationConsumer
	NotificationPublisher
	SchemaManager
	AuthProvider
	SnapshotManager
}

type TransactionKeyType string

const TransactionKey TransactionKeyType = "Transaction"

type Transaction interface {
	Prepare(func(ctx context.Context)) TransactionCommitter
}

type TransactionCommitter interface {
	Commit(context.Context)
}
