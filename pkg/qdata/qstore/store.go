package qstore

import (
	"os"

	"github.com/rqure/qlib/pkg/qdata"
)

func getFromEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func New(opts ...qdata.StoreOpts) *qdata.Store {
	store := new(qdata.Store).ApplyOpts(opts...)

	if len(opts) == 0 {
		opts = []qdata.StoreOpts{
			NotifyOverNats(getFromEnvOrDefault("Q_NATS_URL", "nats://nats:4222")),
			PersistOverRedis(getFromEnvOrDefault("Q_REDIS_URL", "redis://redis:6379"), "", 0, 10),
		}
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
}
