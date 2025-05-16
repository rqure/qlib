package qstore

import (
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qmap"
)

// PersistInMemoryMap configures a simple in-memory map as the persistence layer
// Opts can include:
// 1. snapshot interval (time.Duration)
// 2. snapshot retention count (int)
// 3. snapshot directory (string)
// 4. disable persistence (bool)
func PersistInMemoryMap(opts ...interface{}) qdata.StoreOpts {
	return func(store *qdata.Store) {
		config := qmap.MapConfig{
			SnapshotInterval:  1 * time.Hour,
			SnapshotRetention: 3,
			SnapshotDirectory: "qmap_snapshots",
		}

		// Process any additional options
		for i, opt := range opts {
			switch i {
			case 0:
				// First option can be snapshot interval
				if interval, ok := opt.(time.Duration); ok {
					config.SnapshotInterval = interval
				}
			case 1:
				// Second option can be snapshot retention count
				if retention, ok := opt.(int); ok {
					config.SnapshotRetention = retention
				}
			case 2:
				// Third option can be snapshot directory
				if dir, ok := opt.(string); ok && dir != "" {
					config.SnapshotDirectory = dir
				}
			case 3:
				// Fourth option can be disablePersistence flag
				if disable, ok := opt.(bool); ok {
					config.DisablePersistence = disable
				}
			}
		}

		core := qmap.NewCore(config)

		interactor := qmap.NewStoreInteractor(core)
		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qmap.NewConnector(core))
		} else {
			store.StoreConnector = qmap.NewConnector(core)
		}

		store.StoreInteractor = interactor
	}
}
