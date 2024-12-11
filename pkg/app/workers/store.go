package workers

import (
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

type Store struct {
	Connected     signalslots.Signal
	Disconnected  signalslots.Signal
	SchemaUpdated signalslots.Signal

	store       data.Store
	isConnected bool

	connectionCheckTicker *time.Ticker
	notificationTicker    *time.Ticker
}

func NewStore(store data.Store) *Store {
	return &Store{
		Connected:    signal.New(),
		Disconnected: signal.New(),

		store:       store,
		isConnected: false,

		connectionCheckTicker: time.NewTicker(5 * time.Second),
		notificationTicker:    time.NewTicker(100 * time.Millisecond),
	}
}

func (w *Store) Init(h app.Handle) {
}

func (w *Store) Deinit() {
	w.connectionCheckTicker.Stop()
	w.notificationTicker.Stop()
}

func (w *Store) DoWork() {
	select {
	case <-w.connectionCheckTicker.C:
		w.setConnectionStatus(w.store.IsConnected())

		if !w.IsConnected() {
			w.store.Connect()
			return
		}
	case <-w.notificationTicker.C:
		if w.IsConnected() {
			w.store.ProcessNotifications()
		}
	default:
	}
}

func (w *Store) onConnected() {
	log.Info("[StoreWorker::onConnected] Connection status changed to [CONNECTED]")

	w.Connected.Emit()
}

func (w *Store) onDisconnected() {
	log.Info("[StoreWorker::onDisconnected] Connection status changed to [DISCONNECTED]")

	w.Disconnected.Emit()
}

func (w *Store) setConnectionStatus(connected bool) {
	if w.isConnected == connected {
		return
	}

	w.isConnected = connected
	if connected {
		w.onConnected()
	} else {
		w.onDisconnected()
	}
}

func (w *Store) IsConnected() bool {
	return w.isConnected
}
