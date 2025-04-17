package qnats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

type NatsStoreNotifier struct {
	core            NatsCore
	handle          qcontext.Handle
	appName         string
	callbacks       map[string][]qdata.NotificationCallback
	keepAlive       *time.Ticker
	cancelKeepAlive context.CancelFunc
}

func NewStoreNotifier(core NatsCore) qdata.StoreNotifier {
	consumer := &NatsStoreNotifier{
		core:      core,
		callbacks: map[string][]qdata.NotificationCallback{},
		keepAlive: time.NewTicker(30 * time.Second),
	}

	// Subscribe to connection events
	core.Connected().Connect(consumer.onConnected)
	core.Disconnected().Connect(consumer.onDisconnected)

	return consumer
}

func (me *NatsStoreNotifier) onConnected(args qdata.ConnectedArgs) {
	me.appName = qcontext.GetAppName(args.Ctx)
	me.handle = qcontext.GetHandle(args.Ctx)

	// Subscribe to non-distributed notifications (all instances receive these)
	groupSubject := me.core.GetKeyGenerator().GetNotificationGroupSubject(me.appName)
	me.core.Subscribe(groupSubject, me.handleNotification)

	// Subscribe to distributed notifications (only one instance receives each notification)
	distributedSubject := me.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(me.appName)
	me.core.QueueSubscribe(distributedSubject, me.appName, me.handleNotification)

	// Start keepAliveTask
	ctx, cancel := context.WithCancel(context.Background())
	me.cancelKeepAlive = cancel
	go me.keepAliveTask(ctx)
}

func (me *NatsStoreNotifier) onDisconnected(args qdata.DisconnectedArgs) {
	// Stop keepAliveTask
	if me.cancelKeepAlive != nil {
		me.cancelKeepAlive()
		me.cancelKeepAlive = nil
	}
}

func (me *NatsStoreNotifier) handleNotification(msg *nats.Msg) {
	apiMsg := &qprotobufs.ApiMessage{}
	if err := proto.Unmarshal(msg.Data, apiMsg); err != nil {
		qlog.Error("Failed to unmarshal web message: %v", err)
		return
	}

	var notifPb qprotobufs.DatabaseNotification
	if err := apiMsg.Payload.UnmarshalTo(&notifPb); err != nil {
		qlog.Error("Failed to unmarshal notification: %v", err)
		return
	}

	notif := qnotify.FromPb(&notifPb)
	me.handle.DoInMainThread(func(ctx context.Context) {
		callbacks, ok := me.callbacks[notif.GetToken()]

		if ok {
			for _, cb := range callbacks {
				cb.Fn(ctx, notif)
			}
		}
	})
}

func (me *NatsStoreNotifier) keepAliveTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-me.keepAlive.C:
			me.handle.DoInMainThread(func(ctx context.Context) {
				for token := range me.callbacks {
					me.sendNotify(ctx, qnotify.FromToken(token))
				}
			})
		}
	}
}

func (me *NatsStoreNotifier) sendNotify(ctx context.Context, config qdata.NotificationConfig) error {
	if config.GetServiceId() == "" {
		appName := qcontext.GetAppName(ctx)
		config.SetServiceId(appName)
	}

	msg := &qprotobufs.ApiRuntimeRegisterNotificationRequest{
		Requests: []*qprotobufs.DatabaseNotificationConfig{qnotify.ToConfigPb(config)},
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationRegistrationSubject(), msg)
	if err != nil {
		return err
	}

	var response qprotobufs.ApiRuntimeRegisterNotificationResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if len(response.Tokens) == 0 {
		return errors.New("no tokens returned")
	}

	return nil
}

// Note: Callers are expected to call this method from the main thread
func (me *NatsStoreNotifier) Notify(ctx context.Context, config qdata.NotificationConfig, cb qdata.NotificationCallback) (qdata.NotificationToken, error) {
	tokenId := config.GetToken()
	var err error

	if me.callbacks[tokenId] == nil {
		err = me.sendNotify(ctx, config)
	}

	if err == nil {
		me.callbacks[tokenId] = append(me.callbacks[tokenId], cb)
		return qnotify.NewToken(tokenId, me, cb), nil
	}

	return qnotify.NewToken("", me, nil), fmt.Errorf("failed to register notification: %w", err)
}

// Note: Callers are expected to call this method from the main thread
func (me *NatsStoreNotifier) Unnotify(ctx context.Context, token string) error {
	msg := &qprotobufs.ApiRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationRegistrationSubject(), msg)
	if err != nil {
		return fmt.Errorf("failed to unregister notification: %w", err)
	}

	delete(me.callbacks, token)
	return nil
}

// Note: Callers are expected to call this method from the main thread
func (me *NatsStoreNotifier) UnnotifyCallback(ctx context.Context, token string, cb qdata.NotificationCallback) error {
	if me.callbacks[token] == nil {
		return fmt.Errorf("no callbacks registered for token: %s", token)
	}

	callbacks := []qdata.NotificationCallback{}
	for _, callback := range me.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	if len(callbacks) == 0 {
		return me.Unnotify(ctx, token)
	}

	me.callbacks[token] = callbacks
	return nil
}
