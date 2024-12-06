package qapplication

import "time"

type DatabaseWorkerSignals struct {
	Connected     Signal
	Disconnected  Signal
	SchemaUpdated Signal
}

type DatabaseWorker struct {
	Signals DatabaseWorkerSignals

	db                    IDatabase
	connectionState       ConnectionState_ConnectionStateEnum
	connectionCheckTicker *time.Ticker
	notificationTokens    []INotificationToken
}

func NewDatabaseWorker(db IDatabase) *DatabaseWorker {
	return &DatabaseWorker{
		db:                    db,
		connectionState:       ConnectionState_DISCONNECTED,
		notificationTokens:    []INotificationToken{},
		connectionCheckTicker: time.NewTicker(5 * time.Second),
	}
}

func (w *DatabaseWorker) Init() {
	w.Signals.Connected.Connect(Slot(w.onDatabaseConnected))
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

	w.notificationTokens = []INotificationToken{}

	w.notificationTokens = append(w.notificationTokens, w.db.Notify(&DatabaseNotificationConfig{
		Type:  "Root",
		Field: "SchemaUpdateTrigger",
	}, NewNotificationCallback(w.OnSchemaUpdated)))
}

func (w *DatabaseWorker) setConnectionStatus(connected bool) {
	connectionStatus := ConnectionState_DISCONNECTED
	if connected {
		connectionStatus = ConnectionState_CONNECTED
	}

	if w.connectionState == connectionStatus {
		return
	}

	w.connectionState = connectionStatus
	Info("[DatabaseWorker::setConnectionStatus] Connection status changed to [%s]", connectionStatus.String())
	if connected {
		w.Signals.Connected.Emit()
	} else {
		w.Signals.Disconnected.Emit()
	}
}

func (w *DatabaseWorker) IsConnected() bool {
	return w.connectionState == ConnectionState_CONNECTED
}

func (w *DatabaseWorker) OnSchemaUpdated(*DatabaseNotification) {
	w.Signals.SchemaUpdated.Emit()
}
