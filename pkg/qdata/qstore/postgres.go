package qstore

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qpostgres"
)

// PostgresOptions contains options for PostgreSQL storage
type PostgresOptions struct {
	ConnectionString string
	Cache            qpostgres.Cache
	CacheTTL         time.Duration
}

// PersistOverPostgres configures a Store to use PostgreSQL for persistence
func PersistOverPostgres(address string, opts ...func(*PostgresOptions)) qdata.StoreOpts {
	options := &PostgresOptions{
		ConnectionString: address,
		CacheTTL:         5 * time.Minute, // Default TTL
	}

	// Apply option funcs
	for _, opt := range opts {
		opt(options)
	}

	return func(store *qdata.Store) {
		core := qpostgres.NewCore(qpostgres.PostgresConfig{ConnectionString: options.ConnectionString})

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qpostgres.NewConnector(core))
		} else {
			store.StoreConnector = qpostgres.NewConnector(core)
		}

		// Create the store interactor, now with optional cache support
		store.StoreInteractor = qpostgres.NewStoreInteractor(core, options.Cache, options.CacheTTL)
	}
}

// WithCache adds a cache to PostgreSQL storage
func WithCache(cache qpostgres.Cache) func(*PostgresOptions) {
	return func(opts *PostgresOptions) {
		opts.Cache = cache
	}
}

// WithCacheTTL sets the cache TTL
func WithCacheTTL(ttl time.Duration) func(*PostgresOptions) {
	return func(opts *PostgresOptions) {
		opts.CacheTTL = ttl
	}
}

// WithRedisCacheFromURL creates and adds a Redis cache with the given URL
func WithRedisCacheFromURL(url string, ttl time.Duration) func(*PostgresOptions) {
	return func(opts *PostgresOptions) {
		cache, err := qpostgres.NewRedisCache(qpostgres.CacheConfig{
			Address:           url,
			DefaultExpiration: ttl,
		})
		if err == nil {
			opts.Cache = cache
		}
	}
}

// WithMemcachedCache creates and adds a Memcached cache with the given server list
func WithMemcachedCache(serverList string, ttl time.Duration) func(*PostgresOptions) {
	return func(opts *PostgresOptions) {
		cache, err := qpostgres.NewMemcachedCache(qpostgres.CacheConfig{
			Address:           serverList,
			DefaultExpiration: ttl,
		})
		if err == nil {
			opts.Cache = cache
		}
	}
}
