package workers

import (
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
)

type StoreWorker struct {
	Connected     signalslots.Signal[any]
	Disconnected  signalslots.Signal[any]
	SchemaUpdated signalslots.Signal[any]

	store       data.Store
	isConnected bool

	connectionCheckTicker *time.Ticker
	notificationTicker    *time.Ticker

	notificationTokens []data.NotificationToken

	handle app.Handle
}

func NewStoreWorker(store data.Store) *StoreWorker {
	return &StoreWorker{
		Connected:     signalslots.NewSignal[any](),
		Disconnected:  signalslots.NewSignal[any](),
		SchemaUpdated: signalslots.NewSignal[any](),

		store:       store,
		isConnected: false,

		notificationTokens: []data.NotificationToken{},

		connectionCheckTicker: time.NewTicker(5 * time.Second),
		notificationTicker:    time.NewTicker(100 * time.Millisecond),
	}
}

func (w *StoreWorker) Init(h app.Handle) {
	w.handle = h

	go w.DoWork()
}

func (w *StoreWorker) Deinit() {
	w.connectionCheckTicker.Stop()
	w.notificationTicker.Stop()
}

func (w *StoreWorker) DoWork() {
	w.handle.GetWg().Add(1)
	defer w.handle.GetWg().Done()

	for {
		select {
		case <-w.handle.GetCtx().Done():
			return
		case <-w.connectionCheckTicker.C:
			w.handle.Do(func() {
				w.setConnectionStatus(w.store.IsConnected())

				if !w.IsConnected() {
					w.store.Connect()
					return
				}
			})
		case <-w.notificationTicker.C:
			w.handle.Do(func() {
				if w.IsConnected() {
					w.store.ProcessNotifications()
				}
			})
		}
	}
}

func (w *StoreWorker) onConnected() {
	log.Info("[StoreWorker::onConnected] Connection status changed to [CONNECTED]")

	for _, token := range w.notificationTokens {
		token.Unbind()
	}

	w.notificationTokens = []data.NotificationToken{}

	w.notificationTokens = append(w.notificationTokens, w.store.Notify(
		notification.NewConfig().
			SetEntityType("Root").
			SetFieldName("SchemaUpdateTrigger"),
		notification.NewCallback(w.OnSchemaUpdated)))

	w.Connected.Emit(nil)
}

func (w *StoreWorker) onDisconnected() {
	log.Info("[StoreWorker::onDisconnected] Connection status changed to [DISCONNECTED]")

	w.Disconnected.Emit(nil)
}

func (w *StoreWorker) setConnectionStatus(connected bool) {
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

func (w *StoreWorker) IsConnected() bool {
	return w.isConnected
}

func (w *StoreWorker) OnSchemaUpdated(data.Notification) {
	w.SchemaUpdated.Emit(nil)
}
