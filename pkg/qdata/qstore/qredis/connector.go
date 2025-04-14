package qredis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

// RedisConnector manages the Redis connection
type RedisConnector struct {
	core RedisCore

	connected    qss.Signal[qdata.ConnectedArgs]
	disconnected qss.Signal[qdata.DisconnectedArgs]

	isConnected bool
}

// NewConnector creates a new Redis connector
func NewConnector(core RedisCore) qdata.StoreConnector {
	return &RedisConnector{
		core:         core,
		connected:    qss.New[qdata.ConnectedArgs](),
		disconnected: qss.New[qdata.DisconnectedArgs](),
	}
}

func (me *RedisConnector) setConnected(ctx context.Context, connected bool, err error) {
	if connected == me.isConnected {
		return
	}

	me.isConnected = connected

	if connected {
		me.connected.Emit(qdata.ConnectedArgs{Ctx: ctx})
	} else {
		me.disconnected.Emit(qdata.DisconnectedArgs{Ctx: ctx, Err: err})
	}
}

func (me *RedisConnector) closeClient() {
	if me.core.GetClient() != nil {
		_ = me.core.GetClient().Close()
		me.core.SetClient(nil)
	}
}

// Connect establishes a connection to Redis
func (me *RedisConnector) Connect(ctx context.Context) {
	if me.IsConnected() {
		return
	}

	me.closeClient()

	config := me.core.GetConfig()
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
		PoolSize: config.PoolSize,
	})

	me.core.SetClient(client)

	// Test the connection
	if err := client.Ping(ctx).Err(); err != nil {
		qlog.Error("Failed to connect to Redis: %v", err)
		me.setConnected(ctx, false, err)
		return
	}

	me.setConnected(ctx, true, nil)
}

// Disconnect closes the Redis connection
func (me *RedisConnector) Disconnect(ctx context.Context) {
	me.closeClient()
	me.setConnected(ctx, false, nil)
}

// IsConnected returns the connection status
func (me *RedisConnector) IsConnected() bool {
	return me.isConnected
}

// CheckConnection pings Redis to verify the connection is still alive
func (me *RedisConnector) CheckConnection(ctx context.Context) bool {
	client := me.core.GetClient()

	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		me.closeClient()
		me.setConnected(ctx, false, err)
		return false
	}

	me.setConnected(ctx, true, nil)
	return true
}

// Connected returns the connected signal
func (me *RedisConnector) Connected() qss.Signal[qdata.ConnectedArgs] {
	return me.connected
}

// Disconnected returns the disconnected signal
func (me *RedisConnector) Disconnected() qss.Signal[qdata.DisconnectedArgs] {
	return me.disconnected
}
