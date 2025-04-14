package qredis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisConfig holds Redis connection parameters
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
	PoolSize int
}

// RedisCore defines the interface for Redis core functionality
type RedisCore interface {
	SetClient(client *redis.Client)
	GetClient() *redis.Client
	SetConfig(config RedisConfig)
	GetConfig() RedisConfig
	WithClient(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error
}

type redisCore struct {
	client *redis.Client
	config RedisConfig
}

// NewCore creates a new Redis core with the given configuration
func NewCore(config RedisConfig) RedisCore {
	return &redisCore{config: config}
}

func (me *redisCore) SetClient(client *redis.Client) {
	me.client = client
}

func (me *redisCore) GetClient() *redis.Client {
	return me.client
}

func (me *redisCore) SetConfig(config RedisConfig) {
	me.config = config
}

func (me *redisCore) GetConfig() RedisConfig {
	return me.config
}

// WithClient executes a function with the Redis client
func (me *redisCore) WithClient(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error {
	if me.client == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	return fn(ctx, me.client)
}
