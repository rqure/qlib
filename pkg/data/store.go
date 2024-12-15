package data

import (
	"context"
	"time"
)

type SortedSetMember struct {
	Score  float64
	Member string
}

type Store interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected(context.Context) bool

	CreateSnapshot(context.Context) Snapshot
	RestoreSnapshot(context.Context, Snapshot)

	CreateEntity(ctx context.Context, entityType, parentId, name string)
	GetEntity(ctx context.Context, entityId string) Entity
	SetEntity(ctx context.Context, value Entity)
	DeleteEntity(ctx context.Context, entityId string)

	FindEntities(ctx context.Context, entityType string) []string
	GetEntityTypes(ctx context.Context) []string

	EntityExists(ctx context.Context, entityId string) bool
	FieldExists(ctx context.Context, fieldName, entityType string) bool

	GetEntitySchema(ctx context.Context, entityType string) EntitySchema
	SetEntitySchema(context.Context, EntitySchema)

	Read(context.Context, ...Request)
	Write(context.Context, ...Request)

	Notify(ctx context.Context, config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(ctx context.Context, subscriptionId string)
	UnnotifyCallback(ctx context.Context, subscriptionId string, callback NotificationCallback)
	ProcessNotifications(context.Context)

	TempSet(ctx context.Context, key string, value string, expiration time.Duration) bool
	TempGet(ctx context.Context, key string) string
	TempExpire(ctx context.Context, key string, expiration time.Duration)
	TempDel(ctx context.Context, key string)

	SortedSetAdd(ctx context.Context, key string, member string, score float64) int64
	SortedSetRemove(ctx context.Context, key string, member string) int64
	SortedSetRemoveRangeByRank(ctx context.Context, key string, start, stop int64) int64
	SortedSetRangeByScoreWithScores(ctx context.Context, key string, min, max string) []SortedSetMember
}
