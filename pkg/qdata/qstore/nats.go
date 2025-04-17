package qstore

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qnats"
)

func CommunicateOverNats(address string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		core := qnats.NewCore(qnats.NatsConfig{Address: address})

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qnats.NewConnector(core))
		} else {
			store.StoreConnector = qnats.NewConnector(core)
		}

		store.StoreConnector = qnats.NewConnector(core)
		store.StoreInteractor = qnats.NewStoreInteractor(core)
		store.StoreNotifier = qnats.NewStoreNotifier(core)
	}
}

func PersistOverNats(address string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		core := qnats.NewCore(qnats.NatsConfig{Address: address})

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qnats.NewConnector(core))
		} else {
			store.StoreConnector = qnats.NewConnector(core)
		}

		store.StoreInteractor = qnats.NewStoreInteractor(core)
	}
}

func NotifyOverNats(address string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		core := qnats.NewCore(qnats.NatsConfig{Address: address})

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qnats.NewConnector(core))
		} else {
			store.StoreConnector = qnats.NewConnector(core)
		}

		store.StoreNotifier = qnats.NewStoreNotifier(core)
	}
}

func NotifyOverNatsWithCore(core qnats.NatsCore) qdata.StoreOpts {
	return func(store *qdata.Store) {
		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qnats.NewConnector(core))
		} else {
			store.StoreConnector = qnats.NewConnector(core)
		}

		store.StoreNotifier = qnats.NewStoreNotifier(core)
	}
}
