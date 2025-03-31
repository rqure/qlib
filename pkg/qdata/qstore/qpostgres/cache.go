package qpostgres

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
)

// Cache defines the interface for caching operations
type Cache interface {
	// Get retrieves a cached value by key
	Get(ctx context.Context, key string) ([]byte, bool)

	// Set stores a value in the cache with an optional expiration
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error

	// Delete removes a value from the cache
	Delete(ctx context.Context, key string) error

	// Flush clears all values from the cache
	Flush(ctx context.Context) error

	// Health checks if the cache is operational
	Health(ctx context.Context) bool

	// Close closes the cache connection
	Close() error
}

// CacheConfig contains common configuration for cache implementations
type CacheConfig struct {
	// Address is the connection string for the cache server
	Address string

	// DefaultExpiration is the default TTL for cached items
	DefaultExpiration time.Duration

	// Prefix is added to all cache keys to avoid collisions
	Prefix string
}

// CacheKeyBuilder helps construct cache keys consistently
type CacheKeyBuilder struct {
	prefix string
}

// NewCacheKeyBuilder creates a new key builder with the given prefix
func NewCacheKeyBuilder(prefix string) *CacheKeyBuilder {
	return &CacheKeyBuilder{prefix: prefix}
}

// ForEntity builds a cache key for an entity
func (kb *CacheKeyBuilder) ForEntity(entityId qdata.EntityId) string {
	return kb.prefix + ":entity:" + entityId.AsString()
}

// ForField builds a cache key for a field
func (kb *CacheKeyBuilder) ForField(entityId qdata.EntityId, fieldType qdata.FieldType) string {
	return kb.prefix + ":field:" + entityId.AsString() + ":" + fieldType.AsString()
}

// ForEntitySchema builds a cache key for an entity schema
func (kb *CacheKeyBuilder) ForEntitySchema(entityType qdata.EntityType) string {
	return kb.prefix + ":schema:" + entityType.AsString()
}

// ForFieldSchema builds a cache key for a field schema
func (kb *CacheKeyBuilder) ForFieldSchema(entityType qdata.EntityType, fieldType qdata.FieldType) string {
	return kb.prefix + ":fieldschema:" + entityType.AsString() + ":" + fieldType.AsString()
}
