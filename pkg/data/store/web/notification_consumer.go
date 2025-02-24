package web

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	web "github.com/rqure/qlib/pkg/web/go"
	"google.golang.org/protobuf/types/known/anypb"
)

type NotificationConsumer struct {
	core        Core
	transformer data.Transformer
	callbacks   map[string][]data.NotificationCallback
}

func NewNotificationConsumer(core Core) data.ModifiableNotificationConsumer {
	return &NotificationConsumer{
		core:      core,
		callbacks: map[string][]data.NotificationCallback{},
	}
}

func (n *NotificationConsumer) SetTransformer(t data.Transformer) {
	n.transformer = t
}

func (n *NotificationConsumer) Notify(ctx context.Context, config data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	if config.GetServiceId() == "" {
		config.SetServiceId(app.GetName())
	}

	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeRegisterNotificationRequest{
		Requests: []*protobufs.DatabaseNotificationConfig{notification.ToConfigPb(config)},
	})

	response := n.core.SendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return notification.NewToken("", n, nil)
	}

	var resp protobufs.WebRuntimeRegisterNotificationResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return notification.NewToken("", n, nil)
	}

	if len(resp.Tokens) == 0 {
		return notification.NewToken("", n, nil)
	}

	token := resp.Tokens[0]
	n.callbacks[token] = append(n.callbacks[token], cb)

	return notification.NewToken(token, n, cb)
}

func (n *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeUnregisterNotificationRequest{
		Tokens: []string{token},
	})

	n.core.SendAndWait(ctx, msg)
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

func (n *NotificationConsumer) ProcessNotifications(ctx context.Context) {
	msg := web.NewMessage()
	msg.Header = &protobufs.WebHeader{}
	msg.Payload, _ = anypb.New(&protobufs.WebRuntimeGetNotificationsRequest{})

	response := n.core.SendAndWait(ctx, msg)
	if response == nil {
		log.Error("Received nil response")
		return
	}

	var resp protobufs.WebRuntimeGetNotificationsResponse
	if err := response.Payload.UnmarshalTo(&resp); err != nil {
		log.Error("Failed to unmarshal response: %v", err)
		return
	}

	for _, notif := range resp.Notifications {
		notification := notification.FromPb(notif)
		if callbacks, ok := n.callbacks[notification.GetToken()]; ok {
			for _, cb := range callbacks {
				cb.Fn(ctx, notification)
			}
		}
	}
}
