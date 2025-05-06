package qstore

import (
	"time"

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
// with optional snapshot support
func PersistInMemoryBadger(cacheOpts ...interface{}) qdata.StoreOpts {
	return func(store *qdata.Store) {
		config := qbadger.BadgerConfig{
			Path:     "",
			InMemory: true,
			// Default to snapshots in the current directory if not otherwise specified
			SnapshotDirectory: "./badger_snapshots",
		}

		// Process any additional options
		for i, opt := range cacheOpts {
			switch i {
			case 0:
				// First option can be snapshot directory
				if snapshotDir, ok := opt.(string); ok && snapshotDir != "" {
					config.SnapshotDirectory = snapshotDir
				}
			case 1:
				// Second option can be snapshot interval
				if interval, ok := opt.(time.Duration); ok {
					config.SnapshotInterval = interval
				}
			case 2:
				// Third option can be snapshot retention count
				if retention, ok := opt.(int); ok {
					config.SnapshotRetention = retention
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
