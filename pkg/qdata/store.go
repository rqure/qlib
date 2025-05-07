package qdata

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
)

type ConnectedArgs struct {
	Ctx context.Context
}

type DisconnectedArgs struct {
	Ctx context.Context
	Err error
}

type StoreConnector interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected() bool

	// Checks if the connection is alive
	// This may cause the IsConnected() state to change
	// and emit the Connected/Disconnected signal
	CheckConnection(context.Context) bool

	Connected() qss.Signal[ConnectedArgs]
	Disconnected() qss.Signal[DisconnectedArgs]
}

type PublishNotificationArgs struct {
	Ctx  context.Context
	Curr *Request
	Prev *Request
}

type ReadEventArgs struct {
	Ctx context.Context
	Req *Request
}

type WriteEventArgs struct {
	Ctx context.Context
	Req *Request
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
	CursorId int64 // Tracks the cursor ID for the next page. Negative means no more results.
	NextPage func(ctx context.Context) (*PageResult[T], error)
	Cleanup  func() error // Optional cleanup function to be called when the page is done
}

func (p *PageResult[T]) Next(ctx context.Context) bool {
	if p == nil {
		return false
	}

	// Return true if we still have items in the current page
	if len(p.Items) > 0 {
		return true
	}

	// If there's no next page function or cursor is negative, we're done
	if p.NextPage == nil || p.CursorId < 0 {
		return false
	}

	// Try to fetch the next page
	startTime := time.Now()
	nextResult, err := p.NextPage(ctx)
	qlog.Trace("Fetching next page took %s", time.Since(startTime))
	if err != nil || nextResult == nil {
		p.CursorId = -1
		p.NextPage = nil
		return false
	}

	// Update our state with the next page's data
	p.Items = nextResult.Items
	p.CursorId = nextResult.CursorId
	p.NextPage = nextResult.NextPage
	p.Cleanup = nextResult.Cleanup

	// Return true if we have items in the new page
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

func (p *PageResult[T]) ForEach(ctx context.Context, fn func(item T) bool) {
	defer p.Close()

	for p.Next(ctx) {
		item := p.Get()
		if !fn(item) {
			break
		}
	}
}

func (p *PageResult[T]) Close() error {
	if p == nil {
		return nil
	}

	if p.Cleanup != nil {
		return p.Cleanup()
	}

	return nil
}

type StoreInteractor interface {
	CreateEntity(ctx context.Context, eType EntityType, parentId EntityId, name string) (*Entity, error)
	DeleteEntity(context.Context, EntityId) error

	PrepareQuery(sql string, args ...any) (*PageResult[QueryRow], error)
	FindEntities(entityType EntityType, pageOpts ...PageOpts) (*PageResult[EntityId], error)
	GetEntityTypes(pageOpts ...PageOpts) (*PageResult[EntityType], error)

	EntityExists(context.Context, EntityId) (bool, error)
	FieldExists(context.Context, EntityType, FieldType) (bool, error)

	GetEntitySchema(context.Context, EntityType) (*EntitySchema, error)
	SetEntitySchema(context.Context, *EntitySchema) error
	GetFieldSchema(context.Context, EntityType, FieldType) (*FieldSchema, error)
	SetFieldSchema(context.Context, EntityType, FieldType, *FieldSchema) error

	Read(context.Context, ...*Request) error
	Write(context.Context, ...*Request) error

	InitializeSchema(ctx context.Context) error
	CreateSnapshot(ctx context.Context) (*Snapshot, error)
	RestoreSnapshot(ctx context.Context, ss *Snapshot) error

	PublishNotifications() qss.Signal[PublishNotificationArgs]

	WriteEvent() qss.Signal[WriteEventArgs]
	ReadEvent() qss.Signal[ReadEventArgs]
}

type StoreNotifier interface {
	OnNotification(notifPb *qprotobufs.DatabaseNotification)
	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) (NotificationToken, error)
	Unnotify(ctx context.Context, subscriptionId string) error
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback) error
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
