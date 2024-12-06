package qleadership

import "time"

type ILeaderElectionStore interface {
	TempSet(key string, value string, expiration time.Duration) bool
	TempGet(key string) string
	TempExpire(key string, expiration time.Duration)
	TempDel(key string)

	SortedSetAdd(key string, member string, score float64) int64
	SortedSetRemove(key string, member string) int64
	SortedSetRemoveRangeByRank(key string, start, stop int64) int64
	SortedSetRangeByScoreWithScores(key string, min, max string) []SortedSetMember
}
