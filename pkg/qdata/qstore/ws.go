package qstore

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qstore/qws"
)

// CommunicateOverWebSocket sets up a store to communicate over WebSocket
func CommunicateOverWebSocket(url string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		config := qws.WebSocketConfig{URL: url}
		core := qws.NewCore(config)

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qws.NewConnector(core))
		} else {
			store.StoreConnector = qws.NewConnector(core)
		}

		store.StoreInteractor = qws.NewStoreInteractor(core)
		store.StoreNotifier = qws.NewStoreNotifier(core)
	}
}

// PersistOverWebSocket sets up a store to persist data over WebSocket
func PersistOverWebSocket(url string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		config := qws.WebSocketConfig{URL: url}
		core := qws.NewCore(config)

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qws.NewConnector(core))
		} else {
			store.StoreConnector = qws.NewConnector(core)
		}

		store.StoreInteractor = qws.NewStoreInteractor(core)
	}
}

// NotifyOverWebSocket sets up a store to handle notifications over WebSocket
func NotifyOverWebSocket(url string) qdata.StoreOpts {
	return func(store *qdata.Store) {
		config := qws.WebSocketConfig{URL: url}
		core := qws.NewCore(config)

		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qws.NewConnector(core))
		} else {
			store.StoreConnector = qws.NewConnector(core)
		}

		store.StoreNotifier = qws.NewStoreNotifier(core)
	}
}

// NotifyOverWebSocketWithCore sets up a store to handle notifications using an existing WebSocket core
func NotifyOverWebSocketWithCore(core qws.WebSocketCore) qdata.StoreOpts {
	return func(store *qdata.Store) {
		if store.StoreConnector == nil {
			store.StoreConnector = NewMultiConnector()
		}

		if connector, ok := store.StoreConnector.(MultiConnector); ok {
			connector.AddConnector(qws.NewConnector(core))
		} else {
			store.StoreConnector = qws.NewConnector(core)
		}

		store.StoreNotifier = qws.NewStoreNotifier(core)
	}
}
