package qstore

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qbadger"
)

// PersistOverBadger configures BadgerDB as the persistence layer.
// For in-memory mode, set path to "" and inMemory to true.
// Optionally pass go-cache options (expiration, cleanupInterval) to enable in-memory caching.
func PersistOverBadger(path string, inMemory bool, opts ...interface{}) qdata.StoreOpts {
	return func(store *qdata.Store) {
		config := qbadger.BadgerConfig{
			Path:     path,
			InMemory: inMemory,
		}

		// If there are additional configuration options
		for i, opt := range opts {
			switch i {
			case 0:
				// Handle ValueDir
				if valueDir, ok := opt.(string); ok {
					config.ValueDir = valueDir
				}
			case 1:
				// Handle ValueSize
				if valueSize, ok := opt.(int64); ok {
					config.ValueSize = valueSize
				}
			}
		}

		core := qbadger.NewCore(config)

		interactor := qbadger.NewStoreInteractor(core)
		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qbadger.NewConnector(core))
		} else {
			store.StoreConnector = qbadger.NewConnector(core)
		}

		store.StoreInteractor = interactor
	}
}

// PersistInMemoryBadger configures an in-memory BadgerDB as the persistence layer
func PersistInMemoryBadger(cacheOpts ...interface{}) qdata.StoreOpts {
	allOpts := make([]interface{}, 0, len(cacheOpts)+2)
	allOpts = append(allOpts, "", true) // Empty path and inMemory=true
	allOpts = append(allOpts, cacheOpts...)

	return PersistOverBadger("", true, allOpts...)
}
