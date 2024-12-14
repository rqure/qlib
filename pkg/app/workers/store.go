package workers

import (
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/query"
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

	notificationTokens []data.NotificationToken
}

func NewStore(store data.Store) *Store {
	return &Store{
		Connected:    signal.New(),
		Disconnected: signal.New(),

		store:       store,
		isConnected: false,

		connectionCheckTicker: time.NewTicker(5 * time.Second),
		notificationTicker:    time.NewTicker(100 * time.Millisecond),

		notificationTokens: make([]data.NotificationToken, 0),
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
			w.setConnectionStatus(w.store.IsConnected())
		}
	case <-w.notificationTicker.C:
		if w.IsConnected() {
			w.store.ProcessNotifications()
		}
	default:
	}
}

func (w *Store) onConnected() {
	for _, token := range w.notificationTokens {
		token.Unbind()
	}
	w.notificationTokens = make([]data.NotificationToken, 0)

	services := query.New(w.store).
		ForType("Service").
		Where("ApplicationName").Equals(app.GetName()).
		Execute()

	for _, service := range services {
		logLevel := service.GetField("LogLevel").ReadInt()
		log.SetLevel(log.Level(logLevel))

		w.notificationTokens = append(w.notificationTokens, w.store.Notify(
			notification.NewConfig().
				SetEntityId(service.GetId()).
				SetFieldName("LogLevel").
				SetNotifyOnChange(true),
			notification.NewCallback(w.onLogLevelChanged),
		))
	}

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

func (w *Store) onLogLevelChanged(n data.Notification) {
	level := log.Level(n.GetCurrent().GetValue().GetInt())
	log.SetLevel(level)

	log.Info("[StoreWorker::onLogLevelChanged] Log level changed to [%s]", level.String())
}
