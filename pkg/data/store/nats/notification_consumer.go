package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationConsumer struct {
	core        Core
	transformer data.Transformer
	callbacks   map[string][]data.NotificationCallback

	keepAlive *time.Ticker

	pendingNotifs chan data.Notification
}

func NewNotificationConsumer(core Core) data.ModifiableNotificationConsumer {
	consumer := &NotificationConsumer{
		core:          core,
		callbacks:     map[string][]data.NotificationCallback{},
		keepAlive:     time.NewTicker(30 * time.Second),
		pendingNotifs: make(chan data.Notification, 1024),
	}

	// Subscribe using queue group for the service
	subject := core.GetKeyGenerator().GetNotificationSubject()
	core.QueueSubscribe(subject, consumer.handleNotification)
	return consumer
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
	me.pendingNotifs <- notif
}

func (me *NotificationConsumer) ProcessNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notif := <-me.pendingNotifs:
			if callbacks, ok := me.callbacks[notif.GetToken()]; ok {
				for _, cb := range callbacks {
					cb.Fn(context.Background(), notif)
				}
			}
		case <-me.keepAlive.C:
			// Resend all registered notifications to keep them alive
			for token := range me.callbacks {
				me.sendNotify(ctx, notification.FromToken(token))
			}
		default:
			return
		}
	}
}

func (me *NotificationConsumer) sendNotify(ctx context.Context, config data.NotificationConfig) (string, error) {
	if config.GetServiceId() == "" {
		config.SetServiceId(app.GetName())
	}

	msg := &protobufs.ApiRuntimeRegisterNotificationRequest{
		Requests: []*protobufs.DatabaseNotificationConfig{notification.ToConfigPb(config)},
	}

	resp, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationSubject(), msg)
	if err != nil {
		return "", err
	}

	var response protobufs.ApiRuntimeRegisterNotificationResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return "", err
	}

	if len(response.Tokens) == 0 {
		return "", errors.New("no tokens returned")
	}

	token := response.Tokens[0]
	return token, nil
}

func (me *NotificationConsumer) Notify(ctx context.Context, config data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	tokenId, err := me.sendNotify(ctx, config)

	if err == nil {
		me.callbacks[tokenId] = append(me.callbacks[tokenId], cb)
		return notification.NewToken(tokenId, me, cb)
	} else {
		log.Error("notification registration failed: %v", err)
	}

	return notification.NewToken("", me, nil)
}

func (me *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	msg := &protobufs.ApiRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	_, err := me.core.Request(ctx, me.core.GetKeyGenerator().GetNotificationSubject(), msg)
	if err != nil {
		log.Error("Failed to unregister notification: %v", err)
	}
	delete(me.callbacks, token)
}

func (me *NotificationConsumer) UnnotifyCallback(ctx context.Context, token string, cb data.NotificationCallback) {
	if me.callbacks[token] == nil {
		return
	}

	callbacks := []data.NotificationCallback{}
	for _, callback := range me.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	if len(callbacks) == 0 {
		me.Unnotify(ctx, token)
	} else {
		me.callbacks[token] = callbacks
	}
}
