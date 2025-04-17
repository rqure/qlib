package qstore

import "github.com/rqure/qlib/pkg/qdata"

func New(opts ...qdata.StoreOpts) *qdata.Store {
	store := new(qdata.Store).ApplyOpts(opts...)

	if len(opts) == 0 {
		opts = []qdata.StoreOpts{
			NotifyOverNats("nats://nats:4222"),
			PersistOverRedis("redis://redis:6379", "", 0, 10),
		}
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
}
