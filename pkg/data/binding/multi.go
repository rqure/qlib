package binding

import (
	"time"

	"github.com/rqure/qlib/pkg/data"
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
func (m *MultiBinding) Connect() {
	m.impl.Connect()
}

func (m *MultiBinding) Disconnect() {
	m.impl.Disconnect()
}

func (m *MultiBinding) IsConnected() bool {
	return m.impl.IsConnected()
}

func (m *MultiBinding) CreateSnapshot() data.Snapshot {
	return m.impl.CreateSnapshot()
}

func (m *MultiBinding) RestoreSnapshot(s data.Snapshot) {
	m.impl.RestoreSnapshot(s)
}

func (m *MultiBinding) CreateEntity(entityType, parentId, name string) {
	m.impl.CreateEntity(entityType, parentId, name)
}

func (m *MultiBinding) GetEntity(entityId string) data.Entity {
	return m.impl.GetEntity(entityId)
}

func (m *MultiBinding) SetEntity(value data.Entity) {
	m.impl.SetEntity(value)
}

func (m *MultiBinding) DeleteEntity(entityId string) {
	m.impl.DeleteEntity(entityId)
}

func (m *MultiBinding) FindEntities(entityType string) []string {
	return m.impl.FindEntities(entityType)
}

func (m *MultiBinding) GetEntityTypes() []string {
	return m.impl.GetEntityTypes()
}

func (m *MultiBinding) EntityExists(entityId string) bool {
	return m.impl.EntityExists(entityId)
}

func (m *MultiBinding) FieldExists(fieldName, entityType string) bool {
	return m.impl.FieldExists(fieldName, entityType)
}

func (m *MultiBinding) GetEntitySchema(entityType string) data.EntitySchema {
	return m.impl.GetEntitySchema(entityType)
}

func (m *MultiBinding) SetEntitySchema(schema data.EntitySchema) {
	m.impl.SetEntitySchema(schema)
}

// Queue reads and writes instead of executing immediately
func (m *MultiBinding) Read(reqs ...data.Request) {
	m.readReqs = append(m.readReqs, reqs...)
}

func (m *MultiBinding) Write(reqs ...data.Request) {
	m.writeReqs = append(m.writeReqs, reqs...)
}

func (m *MultiBinding) Notify(config data.NotificationConfig, callback data.NotificationCallback) data.NotificationToken {
	return m.impl.Notify(config, callback)
}

func (m *MultiBinding) Unnotify(subscriptionId string) {
	m.impl.Unnotify(subscriptionId)
}

func (m *MultiBinding) UnnotifyCallback(subscriptionId string, callback data.NotificationCallback) {
	m.impl.UnnotifyCallback(subscriptionId, callback)
}

func (m *MultiBinding) ProcessNotifications() {
	m.impl.ProcessNotifications()
}

func (m *MultiBinding) TempSet(key string, value string, expiration time.Duration) bool {
	return m.impl.TempSet(key, value, expiration)
}

func (m *MultiBinding) TempGet(key string) string {
	return m.impl.TempGet(key)
}

func (m *MultiBinding) TempExpire(key string, expiration time.Duration) {
	m.impl.TempExpire(key, expiration)
}

func (m *MultiBinding) TempDel(key string) {
	m.impl.TempDel(key)
}

func (m *MultiBinding) SortedSetAdd(key string, member string, score float64) int64 {
	return m.impl.SortedSetAdd(key, member, score)
}

func (m *MultiBinding) SortedSetRemove(key string, member string) int64 {
	return m.impl.SortedSetRemove(key, member)
}

func (m *MultiBinding) SortedSetRemoveRangeByRank(key string, start, stop int64) int64 {
	return m.impl.SortedSetRemoveRangeByRank(key, start, stop)
}

func (m *MultiBinding) SortedSetRangeByScoreWithScores(key string, min, max string) []data.SortedSetMember {
	return m.impl.SortedSetRangeByScoreWithScores(key, min, max)
}

// MultiBinding specific methods
func (m *MultiBinding) GetEntityById(entityId string) data.EntityBinding {
	e := m.impl.GetEntity(entityId)
	return NewEntity(m, e.GetId()) // Use MultiBinding as the store
}

func (m *MultiBinding) Commit() {
	if len(m.readReqs) > 0 {
		m.impl.Read(m.readReqs...)
	}

	if len(m.writeReqs) > 0 {
		m.impl.Write(m.writeReqs...)
	}

	m.readReqs = []data.Request{}
	m.writeReqs = []data.Request{}
}
