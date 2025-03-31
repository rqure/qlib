package qpostgres

import (
	"context"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/rqure/qlib/pkg/qlog"
)

// MemcachedCache implements the Cache interface using Memcached
type MemcachedCache struct {
	client   *memcache.Client
	config   CacheConfig
	keymaker *CacheKeyBuilder
}

// NewMemcachedCache creates a new Memcached cache with the provided configuration
func NewMemcachedCache(config CacheConfig) (Cache, error) {
	client := memcache.New(config.Address)

	// Test connection
	if err := client.Ping(); err != nil {
		return nil, err
	}

	if config.Prefix == "" {
		config.Prefix = "qdata"
	}

	return &MemcachedCache{
		client:   client,
		config:   config,
		keymaker: NewCacheKeyBuilder(config.Prefix),
	}, nil
}

func (c *MemcachedCache) Get(ctx context.Context, key string) ([]byte, bool) {
	item, err := c.client.Get(key)
	if err == memcache.ErrCacheMiss {
		return nil, false
	}
	if err != nil {
		qlog.Warn("Memcached cache get error: %v", err)
		return nil, false
	}
	return item.Value, true
}

func (c *MemcachedCache) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	exp := int32(0)
	if expiration > 0 {
		exp = int32(expiration.Seconds())
	} else if c.config.DefaultExpiration > 0 {
		exp = int32(c.config.DefaultExpiration.Seconds())
	}

	return c.client.Set(&memcache.Item{
		Key:        key,
		Value:      value,
		Expiration: exp,
	})
}

func (c *MemcachedCache) Delete(ctx context.Context, key string) error {
	err := c.client.Delete(key)
	if err == memcache.ErrCacheMiss {
		return nil
	}
	return err
}

func (c *MemcachedCache) Flush(ctx context.Context) error {
	return c.client.FlushAll()
}

func (c *MemcachedCache) Health(ctx context.Context) bool {
	return c.client.Ping() == nil
}

func (c *MemcachedCache) Close() error {
	// Memcache client doesn't have a Close method
	return nil
}
