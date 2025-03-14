package qnats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qnotify"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"github.com/rqure/qlib/pkg/qss"
	"github.com/rqure/qlib/pkg/qss/qsignal"
	"google.golang.org/protobuf/proto"
)

type NotificationConsumer struct {
	core            Core
	mu              sync.RWMutex
	callbacks       map[string][]qdata.NotificationCallback
	keepAlive       *time.Ticker
	consumed        qss.Signal
	cancelKeepAlive context.CancelFunc
}

func NewNotificationConsumer(core Core) qdata.ModifiableNotificationConsumer {
	consumer := &NotificationConsumer{
		core:      core,
		callbacks: map[string][]qdata.NotificationCallback{},
		keepAlive: time.NewTicker(30 * time.Second),
		consumed:  qsignal.New(),
	}

	// Subscribe to connection events
	core.Connected().Connect(consumer.onConnected)
	core.Disconnected().Connect(consumer.onDisconnected)

	return consumer
}

func (me *NotificationConsumer) onConnected() {
	// Subscribe to non-distributed notifications (all instances receive these)
	groupSubject := me.core.GetKeyGenerator().GetNotificationGroupSubject(qapp.GetName())
	me.core.Subscribe(groupSubject, me.handleNotification)

	// Subscribe to distributed notifications (only one instance receives each notification)
	distributedSubject := me.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(qapp.GetName())
	me.core.QueueSubscribe(distributedSubject, me.handleNotification)

	// Start keepAliveTask
	ctx, cancel := context.WithCancel(context.Background())
	me.cancelKeepAlive = cancel
	go me.keepAliveTask(ctx)

	me.core.ReadyToConsume().Emit()
}

func (me *NotificationConsumer) onDisconnected(err error) {
	// Stop keepAliveTask
	if me.cancelKeepAlive != nil {
		me.cancelKeepAlive()
		me.cancelKeepAlive = nil
	}
}

func (me *NotificationConsumer) handleNotification(msg *nats.Msg) {
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
	me.consumed.Emit(me.generateInvokeCallbacksFn(notif))
}

func (me *NotificationConsumer) keepAliveTask(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-me.keepAlive.C:
			me.mu.RLock()
			// Resend all registered notifications to keep them alive
			for token := range me.callbacks {
				me.sendNotify(ctx, qnotify.FromToken(token))
			}
			me.mu.RUnlock()
		}
	}
}

func (me *NotificationConsumer) sendNotify(ctx context.Context, config qdata.NotificationConfig) error {
	if config.GetServiceId() == "" {
		config.SetServiceId(qapp.GetName())
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

func (me *NotificationConsumer) Notify(ctx context.Context, config qdata.NotificationConfig, cb qdata.NotificationCallback) qdata.NotificationToken {
	tokenId := config.GetToken()
	var err error

	me.mu.Lock()
	if me.callbacks[tokenId] == nil {
		err = me.sendNotify(ctx, config)
	}

	if err == nil {
		me.callbacks[tokenId] = append(me.callbacks[tokenId], cb)
		me.mu.Unlock()
		return qnotify.NewToken(tokenId, me, cb)
	}
	me.mu.Unlock()

	qlog.Error("notification registration failed: %v", err)
	return qnotify.NewToken("", me, nil)
}

func (me *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	msg := &qprotobufs.ApiRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationRegistrationSubject(), msg)
	if err != nil {
		qlog.Error("Failed to unregister notification: %v", err)
	}
	me.mu.Lock()
	delete(me.callbacks, token)
	me.mu.Unlock()
}

func (me *NotificationConsumer) UnnotifyCallback(ctx context.Context, token string, cb qdata.NotificationCallback) {
	me.mu.Lock()
	if me.callbacks[token] == nil {
		me.mu.Unlock()
		return
	}

	callbacks := []qdata.NotificationCallback{}
	for _, callback := range me.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	if len(callbacks) == 0 {
		me.mu.Unlock()
		me.Unnotify(ctx, token)
		return
	}

	me.callbacks[token] = callbacks
	me.mu.Unlock()
}

func (me *NotificationConsumer) Consumed() qss.Signal {
	return me.consumed
}

func (me *NotificationConsumer) generateInvokeCallbacksFn(notif qdata.Notification) func(context.Context) {
	return func(ctx context.Context) {
		callbacksCopy := []qdata.NotificationCallback{}
		me.mu.RLock()
		callbacks, ok := me.callbacks[notif.GetToken()]
		if ok {
			callbacksCopy = append(callbacksCopy, callbacks...)
		}
		me.mu.RUnlock()

		for _, cb := range callbacksCopy {
			cb.Fn(ctx, notif)
		}
	}
}
