package qbinding

import (
	"context"

	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
)

type MultiBinding struct {
	impl      qdata.Store
	readReqs  []qdata.Request
	writeReqs []qdata.Request
}

func NewMulti(store qdata.Store) qdata.MultiBinding {
	return &MultiBinding{
		impl:      store,
		readReqs:  []qdata.Request{},
		writeReqs: []qdata.Request{},
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

func (m *MultiBinding) Connected() qss.Signal[qss.VoidType] {
	return m.impl.Connected()
}

func (m *MultiBinding) Disconnected() qss.Signal[error] {
	return m.impl.Disconnected()
}

func (m *MultiBinding) CreateSnapshot(ctx context.Context) qdata.Snapshot {
	return m.impl.CreateSnapshot(ctx)
}

func (m *MultiBinding) RestoreSnapshot(ctx context.Context, s qdata.Snapshot) {
	m.impl.RestoreSnapshot(ctx, s)
}

func (m *MultiBinding) CreateEntity(ctx context.Context, entityType, parentId, name string) string {
	return m.impl.CreateEntity(ctx, entityType, parentId, name)
}

func (m *MultiBinding) GetEntity(ctx context.Context, entityId string) qdata.Entity {
	return m.impl.GetEntity(ctx, entityId)
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

func (m *MultiBinding) GetEntitySchema(ctx context.Context, entityType string) qdata.EntitySchema {
	return m.impl.GetEntitySchema(ctx, entityType)
}

func (m *MultiBinding) GetFieldSchema(ctx context.Context, entityType, fieldName string) qdata.FieldSchema {
	return m.impl.GetFieldSchema(ctx, entityType, fieldName)
}

func (m *MultiBinding) SetFieldSchema(ctx context.Context, entityType, fieldName string, schema qdata.FieldSchema) {
	m.impl.SetFieldSchema(ctx, entityType, fieldName, schema)
}

func (m *MultiBinding) PublishNotifications(ctx context.Context, curr qdata.Request, prev qdata.Request) {
	m.impl.PublishNotifications(ctx, curr, prev)
}

func (m *MultiBinding) SetEntitySchema(ctx context.Context, schema qdata.EntitySchema) {
	m.impl.SetEntitySchema(ctx, schema)
}

// Queue reads and writes instead of executing immediately
func (m *MultiBinding) Read(ctx context.Context, reqs ...qdata.Request) {
	m.readReqs = append(m.readReqs, reqs...)
}

func (m *MultiBinding) Write(ctx context.Context, reqs ...qdata.Request) {
	cloned := make([]qdata.Request, len(reqs))
	for i, r := range reqs {
		cloned[i] = r.Clone()
	}
	m.writeReqs = append(m.writeReqs, cloned...)
}

func (m *MultiBinding) Notify(ctx context.Context, config qdata.NotificationConfig, callback qdata.NotificationCallback) qdata.NotificationToken {
	return m.impl.Notify(ctx, config, callback)
}

func (m *MultiBinding) Unnotify(ctx context.Context, subscriptionId string) {
	m.impl.Unnotify(ctx, subscriptionId)
}

func (m *MultiBinding) UnnotifyCallback(ctx context.Context, subscriptionId string, callback qdata.NotificationCallback) {
	m.impl.UnnotifyCallback(ctx, subscriptionId, callback)
}

func (m *MultiBinding) Consumed() qss.Signal[func(context.Context)] {
	return m.impl.Consumed()
}

// MultiBinding specific methods
func (m *MultiBinding) GetEntityById(ctx context.Context, entityId string) qdata.EntityBinding {
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
	if impl, ok := m.impl.(qdata.MultiBinding); ok {
		impl.Commit(ctx)
	}

	m.readReqs = []qdata.Request{}
	m.writeReqs = []qdata.Request{}
}

func (m *MultiBinding) AuthClient(ctx context.Context) qauth.Client {
	return m.impl.AuthClient(ctx)
}

func (m *MultiBinding) InitializeIfRequired(ctx context.Context) {
	m.impl.InitializeIfRequired(ctx)
}

func (m *MultiBinding) GetImpl() qdata.Store {
	if impl, ok := m.impl.(*MultiBinding); ok {
		return impl.GetImpl()
	}
	return m.impl
}

func (m *MultiBinding) FindEntitiesPaginated(ctx context.Context, entityType string, pageSize int) qdata.PaginatedResult {
	return m.impl.FindEntitiesPaginated(ctx, entityType, pageSize)
}
