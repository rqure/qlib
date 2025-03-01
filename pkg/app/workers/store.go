package workers

import (
	"context"

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

func (me *Store) Init(ctx context.Context, handle app.Handle) {
	me.handle = handle

	me.store.Connected().Connect(me.onConnected)
	me.store.Disconnected().Connect(me.onDisconnected)

	me.store.Consumed().Connect(me.onConsumed)
}

func (me *Store) Deinit(context.Context) {
}

func (me *Store) DoWork(ctx context.Context) {
	session := me.store.Session(ctx)
	if session.PastHalfLife(ctx) {
		session.Refresh(ctx)
	}
}

func (me *Store) onConnected(ctx context.Context) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isConnected = true

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

		log.Info("Connection status changed to [CONNECTED]")

		me.Connected.Emit(ctx)

		me.SchemaUpdated.Emit(ctx)
	})
}

func (me *Store) onDisconnected() {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isConnected = false

		log.Info("Connection status changed to [DISCONNECTED]")

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
