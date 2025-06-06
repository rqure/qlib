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

func DefaultWebsocketAddress() string {
	return getFromEnvOrDefault("Q_CORE_WS_URL", "ws://localhost:7860/core/")
}

func New(opts ...qdata.StoreOpts) *qdata.Store {
	if len(opts) == 0 {
		opts = []qdata.StoreOpts{
			CommunicateOverWebSocket(DefaultWebsocketAddress()),
		}
	}

	store := new(qdata.Store).Init(opts...)

	return store
}

func New2() *qdata.Store {
	opts := []qdata.StoreOpts{
		PersistInMemoryMap(),
		NotifyOverWebSocket(DefaultWebsocketAddress()),
	}

	store := new(qdata.Store).Init(opts...)

	return store
}
