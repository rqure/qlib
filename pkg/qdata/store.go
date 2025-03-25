package qdata

import (
	"context"

	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qss"
)

type StoreConnector interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected(context.Context) bool

	Connected() qss.Signal[qss.VoidType]
	Disconnected() qss.Signal[error]
}

type StoreInteractor interface {
	CreateEntity(ctx context.Context, eType EntityType, parentId EntityId, name string) string
	GetEntity(context.Context, EntityId) *Entity
	DeleteEntity(context.Context, EntityId)

	// Find(sql string) Query
	FindEntities(context.Context, EntityType) []EntityId
	GetEntityTypes(context.Context) []EntityType

	EntityExists(context.Context, EntityId) bool
	FieldExists(context.Context, EntityType, FieldType) bool

	GetEntitySchema(context.Context, EntityType) *EntitySchema
	SetEntitySchema(context.Context, *EntitySchema)
	GetFieldSchema(context.Context, EntityType, FieldType) *FieldSchema
	SetFieldSchema(context.Context, EntityType, FieldType, *FieldSchema)

	PublishNotifications(ctx context.Context, curr *Request, prev *Request)

	Read(context.Context, ...*Request)
	Write(context.Context, ...*Request)
}

type FieldAuthorizerKeyType string

const FieldAuthorizerKey FieldAuthorizerKeyType = "Authorizer"

type FieldAuthorizer interface {
	AccessorId() string
	IsAuthorized(ctx context.Context, entityId, fieldName string, forWrite bool) bool
}

type StoreNotifier interface {
	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(ctx context.Context, subscriptionId string)
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback)
	Consumed() qss.Signal[func(context.Context)]
}

type IndirectionResolver interface {
	Resolve(ctx context.Context, entityId string, fields string) (string, string)
}

type AuthProvider interface {
	AuthClient(ctx context.Context) qauth.Client
}

type Store struct {
	StoreConnector
	StoreInteractor
	StoreNotifier
}
