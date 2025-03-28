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

type PublishNotificationArgs struct {
	Ctx  context.Context
	Curr *Request
	Prev *Request
}

type PageConfig struct {
	PageSize int64
	CursorId int64
}

func (me *PageConfig) ApplyOpts(opts ...PageOpts) *PageConfig {
	for _, opt := range opts {
		opt(me)
	}

	return me
}

func (me *PageConfig) IntoOpts() []PageOpts {
	return []PageOpts{
		POPageSize(me.PageSize),
		POCursorId(me.CursorId),
	}
}

func DefaultPageConfig() *PageConfig {
	return &PageConfig{
		PageSize: 100,
		CursorId: 0,
	}
}

type PageOpts func(*PageConfig)

func POPageSize(pageSize int64) PageOpts {
	return func(pc *PageConfig) {
		pc.PageSize = pageSize
	}
}

func POCursorId(cursorId int64) PageOpts {
	return func(pc *PageConfig) {
		pc.CursorId = cursorId
	}
}

type PageResult[T any] struct {
	Items    []T
	HasMore  bool
	NextPage func(ctx context.Context) (*PageResult[T], error)
}

func (p *PageResult[T]) Next(ctx context.Context) bool {
	if len(p.Items) > 0 {
		return true
	}
	if !p.HasMore {
		return false
	}

	nextResult, err := p.NextPage(ctx)
	if err != nil {
		return false
	}

	*p = *nextResult
	return len(p.Items) > 0
}

func (p *PageResult[T]) Get() T {
	if len(p.Items) == 0 {
		var zero T
		return zero
	}
	item := p.Items[0]
	p.Items = p.Items[1:]
	return item
}

type StoreInteractor interface {
	CreateEntity(ctx context.Context, eType EntityType, parentId EntityId, name string) EntityId
	GetEntity(context.Context, EntityId) *Entity
	DeleteEntity(context.Context, EntityId)

	PrepareQuery(sql string, args ...interface{}) *PageResult[*Entity]
	FindEntities(entityType EntityType, pageOpts ...PageOpts) *PageResult[EntityId]
	GetEntityTypes(pageOpts ...PageOpts) *PageResult[EntityType]

	EntityExists(context.Context, EntityId) bool
	FieldExists(context.Context, EntityType, FieldType) bool

	GetEntitySchema(context.Context, EntityType) *EntitySchema
	SetEntitySchema(context.Context, *EntitySchema)
	GetFieldSchema(context.Context, EntityType, FieldType) *FieldSchema
	SetFieldSchema(context.Context, EntityType, FieldType, *FieldSchema)

	PublishNotifications() qss.Signal[PublishNotificationArgs]

	Read(context.Context, ...*Request)
	Write(context.Context, ...*Request)
}

type FieldAuthorizerKeyType string

const FieldAuthorizerKey FieldAuthorizerKeyType = "Authorizer"

type FieldAuthorizer interface {
	AccessorId() EntityId
	IsAuthorized(ctx context.Context, entityId EntityId, fieldType FieldType, forWrite bool) bool
}

type StoreNotifier interface {
	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(ctx context.Context, subscriptionId string)
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback)
	Consumed() qss.Signal[func(context.Context)]
}

type IndirectionResolver interface {
	Resolve(context.Context, EntityId, FieldType) (EntityId, FieldType)
}

type AuthProvider interface {
	AuthClient(ctx context.Context) qauth.Client
}

type StoreOpts func(*Store)

type Store struct {
	StoreConnector
	StoreInteractor
	StoreNotifier
}

func (me *Store) Init(opts ...StoreOpts) *Store {
	return me.ApplyOpts(opts...)
}

func (me *Store) ApplyOpts(opts ...StoreOpts) *Store {
	for _, opt := range opts {
		opt(me)
	}

	return me
}
