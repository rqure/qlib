package data

import "time"

type SortedSetMember struct {
	Score  float64
	Member string
}

type Store interface {
	Connect()
	Disconnect()
	IsConnected() bool

	CreateSnapshot() Snapshot
	RestoreSnapshot(s Snapshot)

	CreateEntity(entityType, parentId, name string)
	GetEntity(entityId string) Entity
	SetEntity(value Entity)
	DeleteEntity(entityId string)

	FindEntities(entityType string) []string
	GetEntityTypes() []string

	EntityExists(entityId string) bool
	FieldExists(fieldName, entityType string) bool

	GetEntitySchema(entityType string) EntitySchema
	SetEntitySchema(EntitySchema)

	Read(...Request)
	Write(...Request)

	Notify(config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(subscriptionId string)
	UnnotifyCallback(subscriptionId string, callback NotificationCallback)
	ProcessNotifications()

	TempSet(key string, value string, expiration time.Duration) bool
	TempGet(key string) string
	TempExpire(key string, expiration time.Duration)
	TempDel(key string)

	SortedSetAdd(key string, member string, score float64) int64
	SortedSetRemove(key string, member string) int64
	SortedSetRemoveRangeByRank(key string, start, stop int64) int64
	SortedSetRangeByScoreWithScores(key string, min, max string) []SortedSetMember
}
