package qstore

import (
	"os"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
)

func getFromEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func DefaultNatsAddress() string {
	return getFromEnvOrDefault("Q_NATS_URL", "nats://nats:4222")
}

func DefaultRedisAddress() string {
	return getFromEnvOrDefault("Q_REDIS_URL", "redis:6379")
}

func New(opts ...qdata.StoreOpts) *qdata.Store {
	if len(opts) == 0 {
		opts = []qdata.StoreOpts{
			CommunicateOverNats(DefaultNatsAddress()),
		}
	}

	store := new(qdata.Store).Init(opts...)

	return store
}

func New2(natsCore qnats.NatsCore) *qdata.Store {
	opts := []qdata.StoreOpts{
		PersistInMemoryBadger(),
		NotifyOverNatsWithCore(natsCore),
	}

	store := new(qdata.Store).Init(opts...)

	return store
}
