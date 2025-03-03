package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
	"google.golang.org/protobuf/proto"
)

type NotificationConsumer struct {
	core            Core
	transformer     data.Transformer
	mu              sync.RWMutex
	callbacks       map[string][]data.NotificationCallback
	keepAlive       *time.Ticker
	consumed        signalslots.Signal
	cancelKeepAlive context.CancelFunc
}

func NewNotificationConsumer(core Core) data.ModifiableNotificationConsumer {
	consumer := &NotificationConsumer{
		core:      core,
		callbacks: map[string][]data.NotificationCallback{},
		keepAlive: time.NewTicker(30 * time.Second),
		consumed:  signal.New(),
	}

	// Subscribe to connection events
	core.Connected().Connect(consumer.onConnected)
	core.Disconnected().Connect(consumer.onDisconnected)

	return consumer
}

func (me *NotificationConsumer) onConnected() {
	// Subscribe to non-distributed notifications (all instances receive these)
	groupSubject := me.core.GetKeyGenerator().GetNotificationGroupSubject(app.GetName())
	me.core.Subscribe(groupSubject, me.handleNotification)

	// Subscribe to distributed notifications (only one instance receives each notification)
	distributedSubject := me.core.GetKeyGenerator().GetDistributedNotificationGroupSubject(app.GetName())
	me.core.QueueSubscribe(distributedSubject, me.handleNotification)

	// Start keepAliveTask
	ctx, cancel := context.WithCancel(context.Background())
	me.cancelKeepAlive = cancel
	go me.keepAliveTask(ctx)
}

func (me *NotificationConsumer) onDisconnected(err error) {
	// Stop keepAliveTask
	if me.cancelKeepAlive != nil {
		me.cancelKeepAlive()
		me.cancelKeepAlive = nil
	}
}

func (me *NotificationConsumer) SetTransformer(t data.Transformer) {
	me.transformer = t
}

func (me *NotificationConsumer) handleNotification(msg *nats.Msg) {
	apiMsg := &protobufs.ApiMessage{}
	if err := proto.Unmarshal(msg.Data, apiMsg); err != nil {
		log.Error("Failed to unmarshal web message: %v", err)
		return
	}

	var notifPb protobufs.DatabaseNotification
	if err := apiMsg.Payload.UnmarshalTo(&notifPb); err != nil {
		log.Error("Failed to unmarshal notification: %v", err)
		return
	}

	notif := notification.FromPb(&notifPb)
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
				me.sendNotify(ctx, notification.FromToken(token))
			}
			me.mu.RUnlock()
		}
	}
}

func (me *NotificationConsumer) sendNotify(ctx context.Context, config data.NotificationConfig) error {
	if config.GetServiceId() == "" {
		config.SetServiceId(app.GetName())
	}

	msg := &protobufs.ApiRuntimeRegisterNotificationRequest{
		Requests: []*protobufs.DatabaseNotificationConfig{notification.ToConfigPb(config)},
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationRegistrationSubject(), msg)
	if err != nil {
		return err
	}

	var response protobufs.ApiRuntimeRegisterNotificationResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return err
	}

	if len(response.Tokens) == 0 {
		return errors.New("no tokens returned")
	}

	return nil
}

func (me *NotificationConsumer) Notify(ctx context.Context, config data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	tokenId := config.GetToken()
	var err error

	me.mu.Lock()
	if me.callbacks[tokenId] == nil {
		err = me.sendNotify(ctx, config)
	}

	if err == nil {
		me.callbacks[tokenId] = append(me.callbacks[tokenId], cb)
		me.mu.Unlock()
		return notification.NewToken(tokenId, me, cb)
	}
	me.mu.Unlock()

	log.Error("notification registration failed: %v", err)
	return notification.NewToken("", me, nil)
}

func (me *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	msg := &protobufs.ApiRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationRegistrationSubject(), msg)
	if err != nil {
		log.Error("Failed to unregister notification: %v", err)
	}
	me.mu.Lock()
	delete(me.callbacks, token)
	me.mu.Unlock()
}

func (me *NotificationConsumer) UnnotifyCallback(ctx context.Context, token string, cb data.NotificationCallback) {
	me.mu.Lock()
	if me.callbacks[token] == nil {
		me.mu.Unlock()
		return
	}

	callbacks := []data.NotificationCallback{}
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

func (me *NotificationConsumer) Consumed() signalslots.Signal {
	return me.consumed
}

func (me *NotificationConsumer) generateInvokeCallbacksFn(notif data.Notification) func(context.Context) {
	return func(ctx context.Context) {
		callbacksCopy := []data.NotificationCallback{}
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
