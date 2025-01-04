package leadership

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/data/store"
)

type Store interface {
	Connect(context.Context)
	Disconnect(context.Context)
	IsConnected(context.Context) bool

	TempSet(ctx context.Context, key string, value string, expiration time.Duration) bool
	TempGet(ctx context.Context, key string) string
	TempExpire(ctx context.Context, key string, expiration time.Duration)
	TempDel(ctx context.Context, key string)

	SortedSetAdd(ctx context.Context, key string, member string, score float64) int64
	SortedSetRemove(ctx context.Context, key string, member string) int64
	SortedSetRemoveRangeByRank(ctx context.Context, key string, start, stop int64) int64
	SortedSetRangeByScoreWithScores(ctx context.Context, key string, min, max string) []store.SortedSetMember
}
