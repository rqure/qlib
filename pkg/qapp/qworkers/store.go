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

type SchemaChangedArgs struct {
	Ctx        context.Context
	EntityType qdata.EntityType
}

type EntityCreateArgs struct {
	Ctx    context.Context
	Entity *qdata.Entity
}

type EntityDeletedArgs struct {
	Ctx    context.Context
	Entity *qdata.Entity
}

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

	SchemaChanged() qss.Signal[SchemaChangedArgs]
	EntityCreated() qss.Signal[EntityCreateArgs]
	EntityDeleted() qss.Signal[EntityDeletedArgs]
}

type storeWorker struct {
	connected    qss.Signal[context.Context]
	disconnected qss.Signal[context.Context]

	authReady    qss.Signal[context.Context]
	authNotReady qss.Signal[context.Context]

	schemaChanged qss.Signal[SchemaChangedArgs]
	entityCreated qss.Signal[EntityCreateArgs]
	entityDeleted qss.Signal[EntityDeletedArgs]

	store            *qdata.Store
	isStoreConnected bool
	isAuthReady      bool
	isReady          bool
	client           *qdata.Entity

	notificationTokens []qdata.NotificationToken

	sessionRefreshTimer    *time.Ticker
	connectionCheckTimer   *time.Ticker
	connectionAttemptTimer *time.Ticker
	metricsTimer           *time.Ticker

	readCount    int
	writeCount   int
	busyDuration time.Duration

	handle qcontext.Handle
}

func NewStore(store *qdata.Store) Store {
	return &storeWorker{
		connected:    qss.New[context.Context](),
		disconnected: qss.New[context.Context](),

		authReady:    qss.New[context.Context](),
		authNotReady: qss.New[context.Context](),

		schemaChanged: qss.New[SchemaChangedArgs](),
		entityCreated: qss.New[EntityCreateArgs](),
		entityDeleted: qss.New[EntityDeletedArgs](),

		store:            store,
		isStoreConnected: false,
		isAuthReady:      false,
		isReady:          false,

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
	me.handle.BusyEvent().Connect(me.onBusyEvent)

	me.sessionRefreshTimer = time.NewTicker(5 * time.Second)
	me.connectionAttemptTimer = time.NewTicker(5 * time.Second)
	me.connectionCheckTimer = time.NewTicker(1 * time.Second)
	me.metricsTimer = time.NewTicker(1 * time.Second)

	me.store.Connected().Connect(me.onConnected)
	me.store.Disconnected().Connect(me.onDisconnected)

	me.store.ReadEvent().Connect(me.onRead)
	me.store.WriteEvent().Connect(me.onWrite)

	me.tryRefreshSession(ctx)
	me.tryConnect(ctx)
}

func (me *storeWorker) Deinit(ctx context.Context) {
	me.sessionRefreshTimer.Stop()
	me.connectionAttemptTimer.Stop()
	me.connectionCheckTimer.Stop()
	me.metricsTimer.Stop()

	me.store.Disconnect(ctx)
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

	select {
	case <-me.metricsTimer.C:
		if me.isReady && me.client != nil {
			me.client.Field(qdata.FTReadsPerSecond).Value.FromInt(me.readCount)
			me.client.Field(qdata.FTWritesPerSecond).Value.FromInt(me.writeCount)
			idleDuration := (1 * time.Second) - me.busyDuration
			me.client.Field(qdata.FTIdleMsPerSecond).Value.FromInt(int(idleDuration.Milliseconds()))

			err := me.store.Write(ctx,
				me.client.Field(qdata.FTReadsPerSecond).AsWriteRequest(),
				me.client.Field(qdata.FTWritesPerSecond).AsWriteRequest(),
				me.client.Field(qdata.FTIdleMsPerSecond).AsWriteRequest())
			if err != nil {
				qlog.Warn("Failed to write metrics: %v", err)
			}

			me.readCount = 0
			me.writeCount = 0
			me.busyDuration = 0
		}
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

func (me *storeWorker) onRead(_ qdata.ReadEventArgs) {
	me.readCount += 1
}

func (me *storeWorker) onWrite(_ qdata.WriteEventArgs) {
	me.writeCount += 1
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
	me.isReady = true
	me.client = nil

	// Clear metrics
	me.readCount = 0
	me.writeCount = 0
	me.busyDuration = 0

	// Clear previous notifications
	for _, token := range me.notificationTokens {
		token.Unbind(ctx)
	}

	// Reinitialize notifications
	me.notificationTokens = make([]qdata.NotificationToken, 0)

	root, err := qdata.NewPathResolver(me.store).Resolve(ctx, "Root")
	if err != nil {
		qlog.Warn("Failed to resolve root: %v", err)
		return
	}

	token, err := me.store.Notify(
		ctx,
		qnotify.NewConfig().
			SetEntityId(root.EntityId).
			SetFieldType(qdata.FTSchemaChanged),
		qnotify.NewCallback(me.onSchemaChanged))
	if err != nil {
		qlog.Warn("Failed to bind to schema change: %v", err)
	} else {
		me.notificationTokens = append(me.notificationTokens, token)
	}

	token, err = me.store.Notify(
		ctx,
		qnotify.NewConfig().
			SetEntityId(root.EntityId).
			SetFieldType(qdata.FTEntityCreated),
		qnotify.NewCallback(me.onEntityCreated))
	if err != nil {
		qlog.Warn("Failed to bind to entity creation: %v", err)
	} else {
		me.notificationTokens = append(me.notificationTokens, token)
	}

	token, err = me.store.Notify(
		ctx,
		qnotify.NewConfig().
			SetEntityId(root.EntityId).
			SetFieldType(qdata.FTEntityDeleted),
		qnotify.NewCallback(me.onEntityDeleted))
	if err != nil {
		qlog.Warn("Failed to bind to entity deletion: %v", err)
	} else {
		me.notificationTokens = append(me.notificationTokens, token)
	}

	appName := qcontext.GetAppName(ctx)
	iter, err := me.store.
		PrepareQuery(`
		SELECT "$EntityId", LogLevel, QLibLogLevel
		FROM Client
		WHERE Name = %q`,
			appName)
	if err != nil {
		qlog.Warn("Failed to prepare query: %v", err)
	} else {
		iter.ForEach(ctx, func(row qdata.QueryRow) bool {
			client := row.AsEntity()
			me.client = client

			logLevel := client.Field(qdata.FTLogLevel).Value.GetChoice() + 1
			qlog.SetLevel(qlog.Level(logLevel))

			token, err := me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityId(client.EntityId).
					SetFieldType(qdata.FTLogLevel).
					SetNotifyOnChange(true),
				qnotify.NewCallback(me.onLogLevelChanged),
			)
			if err != nil {
				qlog.Warn("Failed to bind to log level change: %v", err)
			} else {
				me.notificationTokens = append(me.notificationTokens, token)
			}

			qlibLogLevel := client.Field(qdata.FTQLibLogLevel).Value.GetChoice() + 1
			qlog.SetLibLevel(qlog.Level(qlibLogLevel))

			token, err = me.store.Notify(
				ctx,
				qnotify.NewConfig().
					SetEntityId(client.EntityId).
					SetFieldType(qdata.FTQLibLogLevel).
					SetNotifyOnChange(true),
				qnotify.NewCallback(me.onQLibLogLevelChanged),
			)
			if err != nil {
				qlog.Warn("Failed to bind to QLib log level change: %v", err)
			} else {
				me.notificationTokens = append(me.notificationTokens, token)
			}

			return true
		})
	}
}

func (me *storeWorker) OnNotReady(ctx context.Context) {
	me.isReady = false
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

func (me *storeWorker) SchemaChanged() qss.Signal[SchemaChangedArgs] {
	return me.schemaChanged
}

func (me *storeWorker) EntityCreated() qss.Signal[EntityCreateArgs] {
	return me.entityCreated
}

func (me *storeWorker) EntityDeleted() qss.Signal[EntityDeletedArgs] {
	return me.entityDeleted
}

func (me *storeWorker) onSchemaChanged(ctx context.Context, n qdata.Notification) {
	qlog.Debug("Schema changed for EntityType %s", n.GetCurrent().Value.GetString())
	entityType := qdata.EntityType(n.GetCurrent().Value.GetString())
	me.schemaChanged.Emit(SchemaChangedArgs{
		Ctx:        ctx,
		EntityType: entityType,
	})
}

func (me *storeWorker) onEntityCreated(ctx context.Context, n qdata.Notification) {
	qlog.Debug("Entity created: %s", n.GetCurrent().Value.GetString())

	entityId := n.GetCurrent().Value.GetEntityReference()

	me.entityCreated.Emit(EntityCreateArgs{
		Ctx:    ctx,
		Entity: new(qdata.Entity).Init(entityId),
	})
}

func (me *storeWorker) onEntityDeleted(ctx context.Context, n qdata.Notification) {
	qlog.Debug("Entity deleted: %s", n.GetCurrent().Value.GetString())

	entityId := n.GetCurrent().Value.GetEntityReference()

	me.entityDeleted.Emit(EntityDeletedArgs{
		Ctx:    ctx,
		Entity: new(qdata.Entity).Init(entityId),
	})
}

func (me *storeWorker) onBusyEvent(d time.Duration) {
	me.busyDuration += d
}
