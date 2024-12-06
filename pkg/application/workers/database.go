package qapplication

import (
	"time"

	db "github.com/rqure/qlib/pkg/database"
	qlog "github.com/rqure/qlib/pkg/logging"
	pb "github.com/rqure/qlib/pkg/protobufs"
	ss "github.com/rqure/qlib/pkg/signals"
)

type DatabaseWorkerSignals struct {
	Connected     ss.Signal
	Disconnected  ss.Signal
	SchemaUpdated ss.Signal
}

type DatabaseWorker struct {
	Signals DatabaseWorkerSignals

	db                    db.IDatabase
	isConnected           bool
	connectionCheckTicker *time.Ticker
	notificationTokens    []db.INotificationToken
}

func NewDatabaseWorker(db db.IDatabase) *DatabaseWorker {
	return &DatabaseWorker{
		db:                    db,
		isConnected:           false,
		notificationTokens:    []db.INotificationToken{},
		connectionCheckTicker: time.NewTicker(5 * time.Second),
	}
}

func (w *DatabaseWorker) Init() {
	w.Signals.Connected.Connect(ss.Slot(w.onDatabaseConnected))
}

func (w *DatabaseWorker) Deinit() {

}

func (w *DatabaseWorker) DoWork() {
	select {
	case <-w.connectionCheckTicker.C:
		w.setConnectionStatus(w.db.IsConnected())

		if !w.IsConnected() {
			w.db.Connect()
			return
		}
	default:
	}

	if w.IsConnected() {
		w.db.ProcessNotifications()
	}
}

func (w *DatabaseWorker) onDatabaseConnected() {
	for _, token := range w.notificationTokens {
		token.Unbind()
	}

	w.notificationTokens = []db.INotificationToken{}

	w.notificationTokens = append(w.notificationTokens, w.db.Notify(&pb.DatabaseNotificationConfig{
		Type:  "Root",
		Field: "SchemaUpdateTrigger",
	}, db.NewNotificationCallback(w.OnSchemaUpdated)))
}

func (w *DatabaseWorker) setConnectionStatus(connected bool) {
	if w.isConnected == connected {
		return
	}

	w.isConnected = connected
	if connected {
		qlog.Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [CONNECTED]")
		w.Signals.Connected.Emit()
	} else {
		qlog.Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [DISCONNECTED]")
		w.Signals.Disconnected.Emit()
	}
}

func (w *DatabaseWorker) IsConnected() bool {
	return w.isConnected
}

func (w *DatabaseWorker) OnSchemaUpdated(*pb.DatabaseNotification) {
	w.Signals.SchemaUpdated.Emit()
}
