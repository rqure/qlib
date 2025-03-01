package binding

import (
	"context"

	"github.com/rqure/qlib/pkg/auth"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/signalslots"
)

type MultiBinding struct {
	impl      data.Store
	readReqs  []data.Request
	writeReqs []data.Request
}

func NewMulti(store data.Store) data.MultiBinding {
	return &MultiBinding{
		impl:      store,
		readReqs:  []data.Request{},
		writeReqs: []data.Request{},
	}
}

// Implement Store interface by proxying to impl
func (m *MultiBinding) Connect(ctx context.Context) {
	m.impl.Connect(ctx)
}

func (m *MultiBinding) Disconnect(ctx context.Context) {
	m.impl.Disconnect(ctx)
}

func (m *MultiBinding) IsConnected(ctx context.Context) bool {
	return m.impl.IsConnected(ctx)
}

func (m *MultiBinding) Connected() signalslots.Signal {
	return m.impl.Connected()
}

func (m *MultiBinding) Disconnected() signalslots.Signal {
	return m.impl.Disconnected()
}

func (m *MultiBinding) CreateSnapshot(ctx context.Context) data.Snapshot {
	return m.impl.CreateSnapshot(ctx)
}

func (m *MultiBinding) RestoreSnapshot(ctx context.Context, s data.Snapshot) {
	m.impl.RestoreSnapshot(ctx, s)
}

func (m *MultiBinding) CreateEntity(ctx context.Context, entityType, parentId, name string) string {
	return m.impl.CreateEntity(ctx, entityType, parentId, name)
}

func (m *MultiBinding) GetEntity(ctx context.Context, entityId string) data.Entity {
	return m.impl.GetEntity(ctx, entityId)
}

func (m *MultiBinding) SetEntity(ctx context.Context, value data.Entity) {
	m.impl.SetEntity(ctx, value)
}

func (m *MultiBinding) DeleteEntity(ctx context.Context, entityId string) {
	m.impl.DeleteEntity(ctx, entityId)
}

func (m *MultiBinding) FindEntities(ctx context.Context, entityType string) []string {
	return m.impl.FindEntities(ctx, entityType)
}

func (m *MultiBinding) GetEntityTypes(ctx context.Context) []string {
	return m.impl.GetEntityTypes(ctx)
}

func (m *MultiBinding) EntityExists(ctx context.Context, entityId string) bool {
	return m.impl.EntityExists(ctx, entityId)
}

func (m *MultiBinding) FieldExists(ctx context.Context, fieldName, entityType string) bool {
	return m.impl.FieldExists(ctx, fieldName, entityType)
}

func (m *MultiBinding) GetEntitySchema(ctx context.Context, entityType string) data.EntitySchema {
	return m.impl.GetEntitySchema(ctx, entityType)
}

func (m *MultiBinding) GetFieldSchema(ctx context.Context, fieldName, entityType string) data.FieldSchema {
	return m.impl.GetFieldSchema(ctx, fieldName, entityType)
}

func (m *MultiBinding) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema data.FieldSchema) {
	m.impl.SetFieldSchema(ctx, entityType, fieldName, schema)
}

func (m *MultiBinding) PublishNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	m.impl.PublishNotifications(ctx, curr, prev)
}

func (m *MultiBinding) SetEntitySchema(ctx context.Context, schema data.EntitySchema) {
	m.impl.SetEntitySchema(ctx, schema)
}

// Queue reads and writes instead of executing immediately
func (m *MultiBinding) Read(ctx context.Context, reqs ...data.Request) {
	m.readReqs = append(m.readReqs, reqs...)
}

func (m *MultiBinding) Write(ctx context.Context, reqs ...data.Request) {
	cloned := make([]data.Request, len(reqs))
	for i, r := range reqs {
		cloned[i] = r.Clone()
	}
	m.writeReqs = append(m.writeReqs, cloned...)
}

func (m *MultiBinding) Notify(ctx context.Context, config data.NotificationConfig, callback data.NotificationCallback) data.NotificationToken {
	return m.impl.Notify(ctx, config, callback)
}

func (m *MultiBinding) Unnotify(ctx context.Context, subscriptionId string) {
	m.impl.Unnotify(ctx, subscriptionId)
}

func (m *MultiBinding) UnnotifyCallback(ctx context.Context, subscriptionId string, callback data.NotificationCallback) {
	m.impl.UnnotifyCallback(ctx, subscriptionId, callback)
}

func (m *MultiBinding) Consumed() signalslots.Signal {
	return m.impl.Consumed()
}

// MultiBinding specific methods
func (m *MultiBinding) GetEntityById(ctx context.Context, entityId string) data.EntityBinding {
	e := m.impl.GetEntity(ctx, entityId)
	return NewEntity(ctx, m, e.GetId()) // Use MultiBinding as the store
}

func (m *MultiBinding) Commit(ctx context.Context) {
	if len(m.readReqs) > 0 {
		m.impl.Read(ctx, m.readReqs...)
	}

	if len(m.writeReqs) > 0 {
		m.impl.Write(ctx, m.writeReqs...)
	}

	// Commit any nested MultiBinding
	if impl, ok := m.impl.(data.MultiBinding); ok {
		impl.Commit(ctx)
	}

	m.readReqs = []data.Request{}
	m.writeReqs = []data.Request{}
}

func (m *MultiBinding) Session(ctx context.Context) auth.Session {
	return m.impl.Session(ctx)
}

func (m *MultiBinding) AuthorizedRead(ctx context.Context, authorizer data.FieldAuthorizer, reqs ...data.Request) {
	m.impl.AuthorizedRead(ctx, authorizer, reqs...)
}

func (m *MultiBinding) AuthorizedWrite(ctx context.Context, authorizer data.FieldAuthorizer, reqs ...data.Request) {
	m.impl.AuthorizedWrite(ctx, authorizer, reqs...)
}
