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

	notificationTokens []data.NotificationToken

	sessionRefreshTimer    *time.Timer
	connectionAttemptTimer *time.Timer

	handle app.Handle
}

func NewStore(store data.Store) *Store {
	return &Store{
		Connected:     signal.New(),
		Disconnected:  signal.New(),
		SchemaUpdated: signal.New(),

		store:       store,
		isConnected: false,

		notificationTokens: make([]data.NotificationToken, 0),
	}
}

func (me *Store) Init(ctx context.Context) {
	me.handle = app.GetHandle(ctx)
	me.sessionRefreshTimer = time.NewTimer(5 * time.Second)
	me.connectionAttemptTimer = time.NewTimer(5 * time.Second)

	me.store.Connected().Connect(me.onConnected)
	me.store.Disconnected().Connect(me.onDisconnected)

	me.store.Consumed().Connect(me.onConsumed)

	me.tryRefreshSession(ctx)
	me.tryConnect(ctx)
}

func (me *Store) Deinit(context.Context) {
	me.sessionRefreshTimer.Stop()
	me.connectionAttemptTimer.Stop()
}

func (me *Store) DoWork(ctx context.Context) {
	select {
	case <-me.sessionRefreshTimer.C:
		me.tryRefreshSession(ctx)
	case <-me.connectionAttemptTimer.C:
		me.tryConnect(ctx)
	default:
	}
}

func (me *Store) tryConnect(ctx context.Context) {
	if !me.isConnected {
		log.Info("Trying to connect to the store...")
		me.store.Connect(ctx)
	}
}

func (me *Store) tryRefreshSession(ctx context.Context) {
	client := me.store.AuthClient(ctx)

	if client == nil {
		return
	}

	session := client.GetSession(ctx)

	if session.IsValid(ctx) {
		if session.PastHalfLife(ctx) {
			session.Refresh(ctx)
		}
	}
}

func (me *Store) onConnected() {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isConnected = true

		log.Info("Connection status changed to [CONNECTED]")

		for _, token := range me.notificationTokens {
			token.Unbind(ctx)
		}

		me.notificationTokens = make([]data.NotificationToken, 0)

		me.notificationTokens = append(me.notificationTokens,
			me.store.Notify(
				ctx,
				notification.NewConfig().
					SetEntityType("Root").
					SetFieldName("SchemaUpdateTrigger"),
				notification.NewCallback(func(ctx context.Context, n data.Notification) {
					me.SchemaUpdated.Emit(ctx)
				})),
		)

		services := query.New(me.store).
			Select("LogLevel", "QLibLogLevel").
			From("Service").
			Where("ApplicationName").Equals(app.GetName()).
			Execute(ctx)

		for _, service := range services {
			logLevel := service.GetField("LogLevel").GetInt()
			log.SetLevel(log.Level(logLevel))

			me.notificationTokens = append(me.notificationTokens, me.store.Notify(
				ctx,
				notification.NewConfig().
					SetEntityId(service.GetId()).
					SetFieldName("LogLevel").
					SetNotifyOnChange(true),
				notification.NewCallback(me.onLogLevelChanged),
			))

			qlibLogLevel := service.GetField("QLibLogLevel").GetInt()
			log.SetLibLevel(log.Level(qlibLogLevel))

			me.notificationTokens = append(me.notificationTokens, me.store.Notify(
				ctx,
				notification.NewConfig().
					SetEntityId(service.GetId()).
					SetFieldName("QLibLogLevel").
					SetNotifyOnChange(true),
				notification.NewCallback(me.onQLibLogLevelChanged),
			))
		}

		me.Connected.Emit(ctx)

		me.SchemaUpdated.Emit(ctx)
	})
}

func (me *Store) onDisconnected(err error) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isConnected = false

		log.Info("Connection status changed to [DISCONNECTED] with reason [%v]", err)

		me.Disconnected.Emit()
	})
}

func (me *Store) IsConnected() bool {
	return me.isConnected
}

func (me *Store) onLogLevelChanged(ctx context.Context, n data.Notification) {
	level := log.Level(n.GetCurrent().GetValue().GetInt())
	log.SetLevel(level)

	log.Info("Log level changed to [%s]", level.String())
}

func (me *Store) onQLibLogLevelChanged(ctx context.Context, n data.Notification) {
	level := log.Level(n.GetCurrent().GetValue().GetInt())
	log.SetLibLevel(level)

	log.Info("QLib log level changed to [%s]", level.String())
}

func (me *Store) onConsumed(invokeCallbacksFn func(context.Context)) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		invokeCallbacksFn(ctx)
	})
}
