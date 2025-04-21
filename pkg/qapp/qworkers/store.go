package qworkers

import (
	"context"
	"fmt"
	"time"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type Store interface {
	qapp.Worker

	// Fired when the store is connected
	Connected() qss.Signal[context.Context]
	Disconnected() qss.Signal[context.Context]

	// Fired when the session to the store has authenticated successfully
	AuthReady() qss.Signal[context.Context]
	AuthNotReady() qss.Signal[context.Context]

	// Callback when the store has connected and the session is authenticated
	// The ReadinessWorker will fire this when all readiness criteria are met
	OnReady(context.Context)
	OnNotReady(context.Context)
}

type storeWorker struct {
	connected    qss.Signal[context.Context]
	disconnected qss.Signal[context.Context]

	authReady    qss.Signal[context.Context]
	authNotReady qss.Signal[context.Context]

	store            *qdata.Store
	isStoreConnected bool
	isAuthReady      bool

	notificationTokens []qdata.NotificationToken

	sessionRefreshTimer    *time.Ticker
	connectionCheckTimer   *time.Ticker
	connectionAttemptTimer *time.Ticker

	handle qcontext.Handle
}

func NewStore(store *qdata.Store) Store {
	return &storeWorker{
		connected:    qss.New[context.Context](),
		disconnected: qss.New[context.Context](),

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

func (me *storeWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)
	me.sessionRefreshTimer = time.NewTicker(5 * time.Second)
	me.connectionAttemptTimer = time.NewTicker(5 * time.Second)
	me.connectionCheckTimer = time.NewTicker(1 * time.Second)

	me.store.Connected().Connect(me.onConnected)
	me.store.Disconnected().Connect(me.onDisconnected)

	me.tryRefreshSession(ctx)
	me.tryConnect(ctx)
}

func (me *storeWorker) Deinit(context.Context) {
	me.sessionRefreshTimer.Stop()
	me.connectionAttemptTimer.Stop()
	me.connectionCheckTimer.Stop()
}

func (me *storeWorker) DoWork(ctx context.Context) {
	select {
	case <-me.sessionRefreshTimer.C:
		me.tryRefreshSession(ctx)
	default:
	}

	select {
	case <-me.connectionAttemptTimer.C:
		if !me.isStoreConnected {
			me.tryConnect(ctx)
		}
	default:
	}

	select {
	case <-me.connectionCheckTimer.C:
		me.store.CheckConnection(ctx)
	default:
	}

}

func (me *storeWorker) tryConnect(ctx context.Context) {
	me.store.Connect(ctx)
}

func (me *storeWorker) tryRefreshSession(ctx context.Context) {
	clientProvider := qcontext.GetClientProvider[qauthentication.Client](ctx)
	client := clientProvider.Client(ctx)

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
		} else {
			me.setAuthReadiness(ctx, true, "")
		}
	} else {
		me.setAuthReadiness(ctx, false, "Client auth session is not valid")
	}
}

func (me *storeWorker) onConnected(args qdata.ConnectedArgs) {
	me.isStoreConnected = true

	qlog.Info("Connection status changed to [CONNECTED]")

	me.connected.Emit(args.Ctx)
}

func (me *storeWorker) onDisconnected(args qdata.DisconnectedArgs) {
	me.isStoreConnected = false

	qlog.Info("Connection status changed to [DISCONNECTED] with reason: %v", args.Err)

	me.disconnected.Emit(args.Ctx)
}

func (me *storeWorker) IsConnected() bool {
	return me.isStoreConnected
}

func (me *storeWorker) onLogLevelChanged(ctx context.Context, n qdata.Notification) {
	level := qlog.Level(n.GetCurrent().Value.GetInt())
	qlog.SetLevel(level)

	qlog.Info("Log level changed to [%s]", level.String())
}

func (me *storeWorker) onQLibLogLevelChanged(ctx context.Context, n qdata.Notification) {
	level := qlog.Level(n.GetCurrent().Value.GetInt())
	qlog.SetLibLevel(level)

	qlog.Info("QLib log level changed to [%s]", level.String())
}

func (me *storeWorker) OnReady(ctx context.Context) {
	for _, token := range me.notificationTokens {
		token.Unbind(ctx)
	}

	me.notificationTokens = make([]qdata.NotificationToken, 0)

	appName := qcontext.GetAppName(ctx)
	iter, err := me.store.
		PrepareQuery(`
		SELECT "$EntityId", LogLevel, QLibLogLevel
		FROM Client
		WHERE Name = %q`,
			appName)
	if err != nil {
		qlog.Error("Failed to prepare query: %v", err)
	} else {
		iter.ForEach(ctx, func(row qdata.QueryRow) bool {
			client := row.AsEntity()
			logLevel := client.Field("LogLevel").Value.GetChoice() + 1
			qlog.SetLevel(qlog.Level(logLevel))

			token, err := me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityId(client.EntityId).
					SetFieldType("LogLevel").
					SetNotifyOnChange(true),
				qnotify.NewCallback(me.onLogLevelChanged),
			)
			if err != nil {
				qlog.Error("Failed to bind to log level change: %v", err)
			} else {
				me.notificationTokens = append(me.notificationTokens, token)
			}

			qlibLogLevel := client.Field("QLibLogLevel").Value.GetChoice() + 1
			qlog.SetLibLevel(qlog.Level(qlibLogLevel))

			token, err = me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityId(client.EntityId).
					SetFieldType("QLibLogLevel").
					SetNotifyOnChange(true),
				qnotify.NewCallback(me.onQLibLogLevelChanged),
			)
			if err != nil {
				qlog.Error("Failed to bind to QLib log level change: %v", err)
			} else {
				me.notificationTokens = append(me.notificationTokens, token)
			}

			return true
		})
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
