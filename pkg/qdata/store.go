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
	CreateEntity(ctx context.Context, entityType, parentId, name string) string
	GetEntity(ctx context.Context, entityId string) *Entity
	DeleteEntity(ctx context.Context, entityId string)

	// Find(sql string) Query
	FindEntities(ctx context.Context, entityType string) []string
	GetEntityTypes(ctx context.Context) []string

	EntityExists(ctx context.Context, entityId string) bool
	FieldExists(ctx context.Context, entityType, fieldName string) bool

	GetEntitySchema(ctx context.Context, entityType string) *EntitySchema
	SetEntitySchema(context.Context, *EntitySchema)
	GetFieldSchema(ctx context.Context, entityType, fieldName string) *FieldSchema
	SetFieldSchema(ctx context.Context, entityType, fieldName string, schema *FieldSchema)

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
