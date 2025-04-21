package qworkers

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type OneShot interface {
	Connected() qss.Signal[context.Context]
	Disconnected() qss.Signal[context.Context]
}

type oneShotWorker struct {
	connected    qss.Signal[context.Context]
	disconnected qss.Signal[context.Context]

	store            *qdata.Store
	isStoreConnected bool

	connectionCheckTimer   *time.Timer
	connectionAttemptTimer *time.Timer

	handle qcontext.Handle

	performedOneshot bool
}

func NewOneShot(store *qdata.Store) OneShot {
	return &oneShotWorker{
		connected:        qss.New[context.Context](),
		disconnected:     qss.New[context.Context](),
		store:            store,
		isStoreConnected: false,
		performedOneshot: false,
	}
}

func (me *oneShotWorker) Connected() qss.Signal[context.Context] {
	return me.connected
}

func (me *oneShotWorker) Disconnected() qss.Signal[context.Context] {
	return me.disconnected
}

func (me *oneShotWorker) Init(ctx context.Context) {
	me.handle = qcontext.GetHandle(ctx)

	me.connectionAttemptTimer = time.NewTimer(5 * time.Second)
	me.connectionCheckTimer = time.NewTimer(1 * time.Second)

	me.store.Connected().Connect(me.onConnected)
	me.store.Disconnected().Connect(me.onDisconnected)

	me.tryConnect(ctx)
}

func (me *oneShotWorker) Deinit(ctx context.Context) {
	me.connectionAttemptTimer.Stop()
	me.connectionCheckTimer.Stop()
}

func (me *oneShotWorker) DoWork(ctx context.Context) {
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

func (me *oneShotWorker) tryConnect(ctx context.Context) {
	me.store.Connect(ctx)
}

func (me *oneShotWorker) onConnected(args qdata.ConnectedArgs) {
	me.isStoreConnected = true

	if !me.performedOneshot {
		me.connected.Emit(args.Ctx)
		me.performedOneshot = true

		me.handle.Exit()
	}
}

func (me *oneShotWorker) onDisconnected(args qdata.DisconnectedArgs) {
	if args.Err != nil {
		qlog.Warn("Unexpected disconnected from store: %v", args.Err)
	}

	me.isStoreConnected = false
	me.disconnected.Emit(args.Ctx)
}
