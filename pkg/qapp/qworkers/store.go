package qworkers

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
	"github.com/rqure/qlib/pkg/qss/qsignal"
)

type Store interface {
	qapp.Worker
	Connected() qss.Signal
	Disconnected() qss.Signal
	SchemaUpdated() qss.Signal
}

type storeWorker struct {
	connected     qss.Signal
	disconnected  qss.Signal
	schemaUpdated qss.Signal

	store       qdata.Store
	isConnected bool

	notificationTokens []qdata.NotificationToken

	sessionRefreshTimer    *time.Ticker
	connectionAttemptTimer *time.Ticker

	handle qapp.Handle
}

func NewStore(store qdata.Store) Store {
	return &storeWorker{
		connected:     qsignal.New(),
		disconnected:  qsignal.New(),
		schemaUpdated: qsignal.New(),

		store:       store,
		isConnected: false,

		notificationTokens: make([]qdata.NotificationToken, 0),
	}
}

func (me *storeWorker) Connected() qss.Signal {
	return me.connected
}

func (me *storeWorker) Disconnected() qss.Signal {
	return me.disconnected
}

func (me *storeWorker) SchemaUpdated() qss.Signal {
	return me.schemaUpdated
}

func (me *storeWorker) Init(ctx context.Context) {
	me.handle = qapp.GetHandle(ctx)
	me.sessionRefreshTimer = time.NewTicker(5 * time.Second)
	me.connectionAttemptTimer = time.NewTicker(5 * time.Second)

	me.store.Connected().Connect(me.onConnected)
	me.store.Disconnected().Connect(me.onDisconnected)

	me.store.Consumed().Connect(me.onConsumed)

	me.tryRefreshSession(ctx)
	me.tryConnect(ctx)
}

func (me *storeWorker) Deinit(context.Context) {
	me.sessionRefreshTimer.Stop()
	me.connectionAttemptTimer.Stop()
}

func (me *storeWorker) DoWork(ctx context.Context) {
	select {
	case <-me.sessionRefreshTimer.C:
		me.tryRefreshSession(ctx)
	case <-me.connectionAttemptTimer.C:
		me.tryConnect(ctx)
	default:
	}
}

func (me *storeWorker) tryConnect(ctx context.Context) {
	if !me.isConnected {
		qlog.Info("Trying to connect to the store...")
		me.store.Connect(ctx)
	}
}

func (me *storeWorker) tryRefreshSession(ctx context.Context) {
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

func (me *storeWorker) onConnected() {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isConnected = true

		qlog.Info("Connection status changed to [CONNECTED]")

		for _, token := range me.notificationTokens {
			token.Unbind(ctx)
		}

		me.notificationTokens = make([]qdata.NotificationToken, 0)

		me.notificationTokens = append(me.notificationTokens,
			me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityType("Root").
					SetFieldName("SchemaUpdateTrigger"),
				qnotify.NewCallback(func(ctx context.Context, n qdata.Notification) {
					me.schemaUpdated.Emit(ctx)
				})),
		)

		clients := qquery.New(me.store).
			Select("LogLevel", "QLibLogLevel").
			From("Client").
			Where("Name").Equals(qapp.GetName()).
			Execute(ctx)

		for _, client := range clients {
			logLevel := client.GetField("LogLevel").GetInt()
			qlog.SetLevel(qlog.Level(logLevel))

			me.notificationTokens = append(me.notificationTokens, me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityId(client.GetId()).
					SetFieldName("LogLevel").
					SetNotifyOnChange(true),
				qnotify.NewCallback(me.onLogLevelChanged),
			))

			qlibLogLevel := client.GetField("QLibLogLevel").GetInt()
			qlog.SetLibLevel(qlog.Level(qlibLogLevel))

			me.notificationTokens = append(me.notificationTokens, me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityId(client.GetId()).
					SetFieldName("QLibLogLevel").
					SetNotifyOnChange(true),
				qnotify.NewCallback(me.onQLibLogLevelChanged),
			))
		}

		me.connected.Emit(ctx)

		me.schemaUpdated.Emit(ctx)
	})
}

func (me *storeWorker) onDisconnected(err error) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isConnected = false

		qlog.Info("Connection status changed to [DISCONNECTED] with reason [%v]", err)

		me.disconnected.Emit()
	})
}

func (me *storeWorker) IsConnected() bool {
	return me.isConnected
}

func (me *storeWorker) onLogLevelChanged(ctx context.Context, n qdata.Notification) {
	level := qlog.Level(n.GetCurrent().GetValue().GetInt())
	qlog.SetLevel(level)

	qlog.Info("Log level changed to [%s]", level.String())
}

func (me *storeWorker) onQLibLogLevelChanged(ctx context.Context, n qdata.Notification) {
	level := qlog.Level(n.GetCurrent().GetValue().GetInt())
	qlog.SetLibLevel(level)

	qlog.Info("QLib log level changed to [%s]", level.String())
}

func (me *storeWorker) onConsumed(invokeCallbacksFn func(context.Context)) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		invokeCallbacksFn(ctx)
	})
}
