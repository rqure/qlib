package workers

import (
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
)

type DatabaseWorker struct {
	Connected     signalslots.Signal[any]
	Disconnected  signalslots.Signal[any]
	SchemaUpdated signalslots.Signal[any]

	store                 data.Store
	isConnected           bool
	connectionCheckTicker *time.Ticker
	notificationTicker    *time.Ticker
	notificationTokens    []data.NotificationToken

	handle app.ApplicationHandle
}

func NewDatabaseWorker(store data.Store) app.Worker {
	return &DatabaseWorker{
		store:                 store,
		isConnected:           false,
		notificationTokens:    []data.NotificationToken{},
		connectionCheckTicker: time.NewTicker(5 * time.Second),
		notificationTicker:    time.NewTicker(100 * time.Millisecond),
	}
}

func (w *DatabaseWorker) Init(h app.ApplicationHandle) {
	w.handle = h

	go w.DoWork()
}

func (w *DatabaseWorker) Deinit() {
	w.connectionCheckTicker.Stop()
	w.notificationTicker.Stop()
}

func (w *DatabaseWorker) DoWork() {
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

func (w *DatabaseWorker) onDatabaseConnected() {
	log.Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [CONNECTED]")

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

func (w *DatabaseWorker) onDatabaseDisconnected() {
	log.Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [DISCONNECTED]")

	w.Disconnected.Emit(nil)
}

func (w *DatabaseWorker) setConnectionStatus(connected bool) {
	if w.isConnected == connected {
		return
	}

	w.isConnected = connected
	if connected {
		w.onDatabaseConnected()
	} else {
		w.onDatabaseDisconnected()
	}
}

func (w *DatabaseWorker) IsConnected() bool {
	return w.isConnected
}

func (w *DatabaseWorker) OnSchemaUpdated(data.Notification) {
	w.SchemaUpdated.Emit(nil)
}
