package redis

import (
	"context"
	"encoding/base64"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationConsumer struct {
	core              Core
	transformer       data.Transformer
	callbacks         map[string][]data.NotificationCallback
	lastStreamMessage string
}

func NewNotificationConsumer(core Core) data.ModifiableNotificationConsumer {
	return &NotificationConsumer{
		core:              core,
		callbacks:         map[string][]data.NotificationCallback{},
		lastStreamMessage: "$",
	}
}

func (me *NotificationConsumer) SetTransformer(transformer data.Transformer) {
	me.transformer = transformer
}

func (me *NotificationConsumer) ProcessNotifications(ctx context.Context) {
	me.transformer.ProcessPending()

	r, err := me.core.GetClient().XRead(ctx, &redis.XReadArgs{
		Streams: []string{me.core.GetKeyGen().GetNotificationChannelKey(me.getServiceId()), me.lastStreamMessage},
		Count:   1000,
		Block:   -1,
	}).Result()

	if err != nil && err != redis.Nil {
		log.Error("Failed to read stream %v: %v", me.core.GetKeyGen().GetNotificationChannelKey(me.getServiceId()), err)
		return
	}

	for _, x := range r {
		for _, m := range x.Messages {
			me.lastStreamMessage = m.ID
			if data, ok := m.Values["data"].(string); ok {
				if n := me.decodeNotification(data); n != nil {
					for _, callback := range me.callbacks[n.Token] {
						callback.Fn(ctx, notification.FromPb(n))
					}
				}
			}
		}
	}
}

func (me *NotificationConsumer) Notify(ctx context.Context, nc data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	if nc.GetServiceId() == "" {
		nc.SetServiceId(me.getServiceId())
	}

	token := nc.GetToken()
	b, err := proto.Marshal(notification.ToConfigPb(nc))
	if err != nil {
		log.Error("Failed to marshal notification config: %v", err)
		return notification.NewToken("", me, nil)
	}

	encodedConfig := base64.StdEncoding.EncodeToString(b)
	key := me.getNotificationKey(nc)

	if key != "" {
		me.core.GetClient().SAdd(ctx, key, encodedConfig)
		me.callbacks[token] = append(me.callbacks[token], cb)
		return notification.NewToken(token, me, cb)
	}

	return notification.NewToken("", me, nil)
}

func (me *NotificationConsumer) decodeNotification(data string) *protobufs.DatabaseNotification {
	b, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		log.Error("Failed to decode notification: %v", err)
		return nil
	}

	n := &protobufs.DatabaseNotification{}
	if err := proto.Unmarshal(b, n); err != nil {
		log.Error("Failed to unmarshal notification: %v", err)
		return nil
	}

	return n
}

func (me *NotificationConsumer) getServiceId() string {
	return app.GetName()
}

func (me *NotificationConsumer) getNotificationKey(nc data.NotificationConfig) string {
	if nc.GetEntityId() != "" {
		return me.core.GetKeyGen().GetEntityIdNotificationConfigKey(nc.GetEntityId(), nc.GetFieldName())
	}
	if nc.GetEntityType() != "" {
		return me.core.GetKeyGen().GetEntityTypeNotificationConfigKey(nc.GetEntityType(), nc.GetFieldName())
	}
	return ""
}

func (me *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	if me.callbacks[token] != nil {
		delete(me.callbacks, token)
	}
}

func (me *NotificationConsumer) UnnotifyCallback(ctx context.Context, token string, callback data.NotificationCallback) {
	if me.callbacks[token] == nil {
		return
	}

	callbacks := []data.NotificationCallback{}
	for _, cb := range me.callbacks[token] {
		if cb.Id() != callback.Id() {
			callbacks = append(callbacks, cb)
		}
	}

	if len(callbacks) == 0 {
		me.Unnotify(ctx, token)
	} else {
		me.callbacks[token] = callbacks
	}
}
