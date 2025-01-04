package workers

import (
	"context"
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

func (w *Store) Init(context.Context, app.Handle) {
}

func (w *Store) Deinit(context.Context) {
	w.connectionCheckTicker.Stop()
	w.notificationTicker.Stop()
}

func (w *Store) DoWork(ctx context.Context) {
	select {
	case <-w.connectionCheckTicker.C:
		w.setConnectionStatus(ctx, w.store.IsConnected(ctx))

		if !w.IsConnected() {
			w.store.Connect(ctx)
			w.setConnectionStatus(ctx, w.store.IsConnected(ctx))
		}
	case <-w.notificationTicker.C:
		if w.IsConnected() {
			w.store.ProcessNotifications(ctx)
		}
	default:
	}
}

func (w *Store) onConnected(ctx context.Context) {
	for _, token := range w.notificationTokens {
		token.Unbind(ctx)
	}

	w.notificationTokens = make([]data.NotificationToken, 0)

	w.notificationTokens = append(w.notificationTokens,
		w.store.Notify(
			ctx,
			notification.NewConfig().
				SetEntityType("Root").
				SetFieldName("SchemaUpdateTrigger"),
			notification.NewCallback(func(ctx context.Context, n data.Notification) {
				w.SchemaUpdated.Emit(ctx)
			})),
	)

	services := query.New(w.store).
		Select("LogLevel", "QLibLogLevel").
		From("Service").
		Where("ApplicationName").Equals(app.GetName()).
		Execute(ctx)

	for _, service := range services {
		logLevel := service.GetField("LogLevel").GetInt()
		log.SetLevel(log.Level(logLevel))

		w.notificationTokens = append(w.notificationTokens, w.store.Notify(
			ctx,
			notification.NewConfig().
				SetEntityId(service.GetId()).
				SetFieldName("LogLevel").
				SetNotifyOnChange(true),
			notification.NewCallback(w.onLogLevelChanged),
		))

		qlibLogLevel := service.GetField("QLibLogLevel").GetInt()
		log.SetLibLevel(log.Level(qlibLogLevel))

		w.notificationTokens = append(w.notificationTokens, w.store.Notify(
			ctx,
			notification.NewConfig().
				SetEntityId(service.GetId()).
				SetFieldName("QLibLogLevel").
				SetNotifyOnChange(true),
			notification.NewCallback(w.onQLibLogLevelChanged),
		))
	}

	log.Info("Connection status changed to [CONNECTED]")

	w.Connected.Emit(ctx)

	w.SchemaUpdated.Emit(ctx)
}

func (w *Store) onDisconnected() {
	log.Info("Connection status changed to [DISCONNECTED]")

	w.Disconnected.Emit()
}

func (w *Store) setConnectionStatus(ctx context.Context, connected bool) {
	if w.isConnected == connected {
		return
	}

	w.isConnected = connected
	if connected {
		w.onConnected(ctx)
	} else {
		w.onDisconnected()
	}
}

func (w *Store) IsConnected() bool {
	return w.isConnected
}

func (w *Store) onLogLevelChanged(ctx context.Context, n data.Notification) {
	level := log.Level(n.GetCurrent().GetValue().GetInt())
	log.SetLevel(level)

	log.Info("Log level changed to [%s]", level.String())
}

func (w *Store) onQLibLogLevelChanged(ctx context.Context, n data.Notification) {
	level := log.Level(n.GetCurrent().GetValue().GetInt())
	log.SetLibLevel(level)

	log.Info("QLib log level changed to [%s]", level.String())
}
