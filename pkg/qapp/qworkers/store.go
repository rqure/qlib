package qworkers

import (
	"context"
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qdata/qquery"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type Store interface {
	qapp.Worker

	// Fired when the store is connected
	Connected() qss.Signal[context.Context]
	Disconnected() qss.Signal[context.Context]
	SchemaUpdated() qss.Signal[context.Context]

	// Fired when the session to the store has authenticated successfully
	AuthReady() qss.Signal[context.Context]
	AuthNotReady() qss.Signal[context.Context]

	// Callback when the store has connected and the session is authenticated
	// The ReadinessWorker will fire this when all readiness criteria are met
	OnReady(context.Context)
	OnNotReady(context.Context)
}

type storeWorker struct {
	connected     qss.Signal[context.Context]
	disconnected  qss.Signal[context.Context]
	schemaUpdated qss.Signal[context.Context]

	authReady    qss.Signal[context.Context]
	authNotReady qss.Signal[context.Context]

	store            qdata.Store
	isStoreConnected bool
	isAuthReady      bool

	notificationTokens []qdata.NotificationToken

	sessionRefreshTimer    *time.Ticker
	connectionAttemptTimer *time.Ticker

	handle qapp.Handle
}

func NewStore(store qdata.Store) Store {
	return &storeWorker{
		connected:     qss.New[context.Context](),
		disconnected:  qss.New[context.Context](),
		schemaUpdated: qss.New[context.Context](),

		authReady:    qss.New[context.Context](),
		authNotReady: qss.New[context.Context](),

		store:            store,
		isStoreConnected: false,
		isAuthReady:      false,

		notificationTokens: make([]qdata.NotificationToken, 0),
	}
}

func (me *storeWorker) Connected() qss.Signal[context.Context] {
	return me.connected
}

func (me *storeWorker) Disconnected() qss.Signal[context.Context] {
	return me.disconnected
}

func (me *storeWorker) SchemaUpdated() qss.Signal[context.Context] {
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
	default:
	}

	if !me.isStoreConnected {
		select {
		case <-me.connectionAttemptTimer.C:
			me.tryConnect(ctx)
		default:
		}
	}
}

func (me *storeWorker) tryConnect(ctx context.Context) {
	qlog.Info("Trying to connect to the store...")
	me.store.Connect(ctx)
}

func (me *storeWorker) tryRefreshSession(ctx context.Context) {
	client := me.store.AuthClient(ctx)

	if client == nil {
		me.setAuthReadiness(ctx, false, "Failed to get auth client")
		return
	}

	session := client.GetSession(ctx)

	if session.IsValid(ctx) {
		if session.PastHalfLife(ctx) {
			err := session.Refresh(ctx)
			if err != nil {
				me.setAuthReadiness(ctx, false, fmt.Sprintf("Failed to refresh session: %v", err))
			} else {
				me.setAuthReadiness(ctx, true, "")
			}
		}
	} else {
		me.setAuthReadiness(ctx, false, "Client auth session is not valid")
	}
}

func (me *storeWorker) onConnected(qss.VoidType) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isStoreConnected = true

		qlog.Info("Connection status changed to [CONNECTED]")

		me.connected.Emit(ctx)
		me.schemaUpdated.Emit(ctx)
	})
}

func (me *storeWorker) onDisconnected(err error) {
	me.handle.DoInMainThread(func(ctx context.Context) {
		me.isStoreConnected = false

		qlog.Info("Connection status changed to [DISCONNECTED] with reason [%v]", err)

		me.disconnected.Emit(ctx)
	})
}

func (me *storeWorker) IsConnected() bool {
	return me.isStoreConnected
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

func (me *storeWorker) OnReady(ctx context.Context) {
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
		logLevel := client.GetField("LogLevel").GetChoice().Index() + 1
		qlog.SetLevel(qlog.Level(logLevel))

		me.notificationTokens = append(me.notificationTokens, me.store.Notify(
			ctx,
			qnotify.NewConfig().
				SetEntityId(client.GetId()).
				SetFieldName("LogLevel").
				SetNotifyOnChange(true),
			qnotify.NewCallback(me.onLogLevelChanged),
		))

		qlibLogLevel := client.GetField("QLibLogLevel").GetChoice().Index() + 1
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
}

func (me *storeWorker) OnNotReady(ctx context.Context) {

}

func (me *storeWorker) AuthReady() qss.Signal[context.Context] {
	return me.authReady
}

func (me *storeWorker) AuthNotReady() qss.Signal[context.Context] {
	return me.authNotReady
}

func (me *storeWorker) setAuthReadiness(ctx context.Context, ready bool, reason string) {
	if me.isAuthReady == ready {
		return
	}

	me.isAuthReady = ready

	if ready {
		qlog.Info("Authentication status changed to [READY]")
		me.authReady.Emit(ctx)
	} else {
		qlog.Warn("Authentication status changed to [NOT READY] with reason: %s", reason)
		me.authNotReady.Emit(ctx)
	}
}
