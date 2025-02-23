package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
)

type Connector struct {
	core Core
}

func NewConnector(core Core) data.Connector {
	return &Connector{core: core}
}

func (me *Connector) Connect(ctx context.Context) {
	if me.IsConnected(ctx) {
		return
	}

	me.Disconnect(ctx)

	log.Info("Connecting to %v", me.core.GetConfig().Address)
	client := redis.NewClient(&redis.Options{
		Addr:     me.core.GetConfig().Address,
		Password: me.core.GetConfig().Password,
	})

	me.core.SetClient(client)
}

func (me *Connector) Disconnect(ctx context.Context) {
	if client := me.core.GetClient(); client != nil {
		client.Close()
		me.core.SetClient(nil)
	}
}

func (me *Connector) IsConnected(ctx context.Context) bool {
	if client := me.core.GetClient(); client != nil {
		return client.Ping(ctx).Err() == nil
	}
	return false
}
