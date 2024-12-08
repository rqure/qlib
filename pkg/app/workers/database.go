package workers

import (
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
)

type DatabaseWorkerSignals struct {
	Connected     signalslots.Signal
	Disconnected  signalslots.Signal
	SchemaUpdated signalslots.Signal
}

func NewDatabaseWorkerSignals() DatabaseWorkerSignals {
	return DatabaseWorkerSignals{
		Connected:     signalslots.NewSignal(),
		Disconnected:  signalslots.NewSignal(),
		SchemaUpdated: signalslots.NewSignal(),
	}
}

type DatabaseWorker struct {
	Signals DatabaseWorkerSignals

	store                 data.Store
	isConnected           bool
	connectionCheckTicker *time.Ticker
	notificationTokens    []data.NotificationToken
}

func NewDatabaseWorker(store data.Store) app.Worker {
	return &DatabaseWorker{
		store:                 store,
		isConnected:           false,
		notificationTokens:    []data.NotificationToken{},
		connectionCheckTicker: time.NewTicker(5 * time.Second),
	}
}

func (w *DatabaseWorker) Init() {
	w.Signals.Connected.Connect(signalslots.NewSlot(w.onDatabaseConnected))
}

func (w *DatabaseWorker) Deinit() {

}

func (w *DatabaseWorker) DoWork() {
	select {
	case <-w.connectionCheckTicker.C:
		w.setConnectionStatus(w.store.IsConnected())

		if !w.IsConnected() {
			w.store.Connect()
			return
		}
	default:
	}

	if w.IsConnected() {
		w.store.ProcessNotifications()
	}
}

func (w *DatabaseWorker) onDatabaseConnected() {
	for _, token := range w.notificationTokens {
		token.Unbind()
	}

	w.notificationTokens = []data.NotificationToken{}

	w.notificationTokens = append(w.notificationTokens, w.store.Notify(
		notification.NewConfig().
			SetEntityType("Root").
			SetFieldName("SchemaUpdateTrigger"),
		notification.NewCallback(w.OnSchemaUpdated)))
}

func (w *DatabaseWorker) setConnectionStatus(connected bool) {
	if w.isConnected == connected {
		return
	}

	w.isConnected = connected
	if connected {
		log.Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [CONNECTED]")
		w.Signals.Connected.Emit()
	} else {
		log.Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [DISCONNECTED]")
		w.Signals.Disconnected.Emit()
	}
}

func (w *DatabaseWorker) IsConnected() bool {
	return w.isConnected
}

func (w *DatabaseWorker) OnSchemaUpdated(data.Notification) {
	w.Signals.SchemaUpdated.Emit()
}
