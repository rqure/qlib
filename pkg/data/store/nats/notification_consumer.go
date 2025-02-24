package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationConsumer struct {
	core          Core
	transformer   data.Transformer
	callbacks     map[string][]data.NotificationCallback
	pendingNotifs chan data.Notification
}

func NewNotificationConsumer(core Core) data.ModifiableNotificationConsumer {
	consumer := &NotificationConsumer{
		core:          core,
		callbacks:     map[string][]data.NotificationCallback{},
		pendingNotifs: make(chan data.Notification, 1024),
	}

	// Subscribe using queue group for the service
	subject := core.GetKeyGenerator().GetNotificationSubject()
	queue := core.GetKeyGenerator().GetNotificationQueueGroup(app.GetName())
	core.QueueSubscribe(subject, queue, consumer.handleNotification)
	return consumer
}

func (n *NotificationConsumer) SetTransformer(t data.Transformer) {
	n.transformer = t
}

func (n *NotificationConsumer) handleNotification(msg *nats.Msg) {
	webMsg := &protobufs.WebMessage{}
	if err := proto.Unmarshal(msg.Data, webMsg); err != nil {
		log.Error("Failed to unmarshal web message: %v", err)
		return
	}

	var notifPb protobufs.DatabaseNotification
	if err := webMsg.Payload.UnmarshalTo(&notifPb); err != nil {
		log.Error("Failed to unmarshal notification: %v", err)
		return
	}

	notif := notification.FromPb(&notifPb)
	n.pendingNotifs <- notif
}

func (n *NotificationConsumer) ProcessNotifications(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case notif := <-n.pendingNotifs:
			if callbacks, ok := n.callbacks[notif.GetToken()]; ok {
				for _, cb := range callbacks {
					cb.Fn(context.Background(), notif)
				}
			}
		default:
			return
		}
	}
}

func (n *NotificationConsumer) Notify(ctx context.Context, config data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	msg := &protobufs.WebRuntimeRegisterNotificationRequest{
		Requests: []*protobufs.DatabaseNotificationConfig{notification.ToConfigPb(config)},
	}

	resp, err := n.core.Request(ctx, n.core.GetKeyGenerator().GetNotificationRegisterSubject(), msg)
	if err != nil {
		return notification.NewToken("", n, nil)
	}

	var response protobufs.WebRuntimeRegisterNotificationResponse
	if err := resp.Payload.UnmarshalTo(&response); err != nil {
		return notification.NewToken("", n, nil)
	}

	if len(response.Tokens) == 0 {
		return notification.NewToken("", n, nil)
	}

	token := response.Tokens[0]
	n.callbacks[token] = append(n.callbacks[token], cb)
	return notification.NewToken(token, n, cb)
}

func (n *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	msg := &protobufs.WebRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	}

	n.core.Publish(n.core.GetKeyGenerator().GetNotificationUnregisterSubject(), msg)
	delete(n.callbacks, token)
}

func (n *NotificationConsumer) UnnotifyCallback(ctx context.Context, token string, cb data.NotificationCallback) {
	if n.callbacks[token] == nil {
		return
	}

	callbacks := []data.NotificationCallback{}
	for _, callback := range n.callbacks[token] {
		if callback.Id() != cb.Id() {
			callbacks = append(callbacks, callback)
		}
	}

	if len(callbacks) == 0 {
		n.Unnotify(ctx, token)
	} else {
		n.callbacks[token] = callbacks
	}
}
