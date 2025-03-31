package qpostgres

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/qlog"
)

// RedisCache implements the Cache interface using Redis
type RedisCache struct {
	client   *redis.Client
	config   CacheConfig
	keymaker *CacheKeyBuilder
}

// NewRedisCache creates a new Redis cache with the provided configuration
func NewRedisCache(config CacheConfig) (Cache, error) {
	opts, err := redis.ParseURL(config.Address)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	if config.Prefix == "" {
		config.Prefix = "qdata"
	}

	return &RedisCache{
		client:   client,
		config:   config,
		keymaker: NewCacheKeyBuilder(config.Prefix),
	}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string) ([]byte, bool) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false
	}
	if err != nil {
		qlog.Warn("Redis cache get error: %v", err)
		return nil, false
	}
	return val, true
}

func (c *RedisCache) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	if expiration == 0 {
		expiration = c.config.DefaultExpiration
	}
	return c.client.Set(ctx, key, value, expiration).Err()
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

func (c *RedisCache) Flush(ctx context.Context) error {
	return c.client.FlushDB(ctx).Err()
}

func (c *RedisCache) Health(ctx context.Context) bool {
	return c.client.Ping(ctx).Err() == nil
}

func (c *RedisCache) Close() error {
	return c.client.Close()
}
