package redis

import (
	"context"
	"encoding/base64"

	"github.com/redis/go-redis/v9"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/data/request"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationPublisher struct {
	core          Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewNotificationPublisher(core Core) data.ModifiableNotificationPublisher {
	return &NotificationPublisher{core: core}
}

func (me *NotificationPublisher) SetEntityManager(manager data.EntityManager) {
	me.entityManager = manager
}

func (me *NotificationPublisher) SetFieldOperator(operator data.FieldOperator) {
	me.fieldOperator = operator
}

func (me *NotificationPublisher) PublishNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	// Get notification configs for entity ID
	members, err := me.core.GetClient().SMembers(ctx, me.core.GetKeyGen().GetEntityIdNotificationConfigKey(curr.GetEntityId(), curr.GetFieldName())).Result()
	if err != nil && err != redis.Nil {
		log.Error("Failed to get notification configs: %v", err)
		return
	}

	me.publishToMembers(ctx, members, curr, prev)

	// Get notification configs for entity type
	entity := me.entityManager.GetEntity(ctx, curr.GetEntityId())
	if entity == nil {
		return
	}

	members, err = me.core.GetClient().SMembers(ctx, me.core.GetKeyGen().GetEntityTypeNotificationConfigKey(entity.GetType(), curr.GetFieldName())).Result()
	if err != nil && err != redis.Nil {
		log.Error("Failed to get notification configs: %v", err)
		return
	}

	me.publishToMembers(ctx, members, curr, prev)
}

func (me *NotificationPublisher) publishToMembers(ctx context.Context, members []string, curr data.Request, prev data.Request) {
	for _, member := range members {
		config := me.decodeConfig(member)
		if config == nil {
			continue
		}

		if config.NotifyOnChange && proto.Equal(field.ToAnyPb(curr.GetValue()), field.ToAnyPb(prev.GetValue())) {
			continue
		}

		n := &protobufs.DatabaseNotification{
			Token:    member,
			Current:  field.ToFieldPb(field.FromRequest(curr)),
			Previous: field.ToFieldPb(field.FromRequest(prev)),
			Context:  []*protobufs.DatabaseField{},
		}

		// Add context fields if specified
		for _, contextField := range config.ContextFields {
			r := request.New().SetEntityId(curr.GetEntityId()).SetFieldName(contextField)
			me.fieldOperator.Read(ctx, r)
			if r.IsSuccessful() {
				n.Context = append(n.Context, field.ToFieldPb(field.FromRequest(r)))
			}
		}

		notifBytes, err := proto.Marshal(n)
		if err != nil {
			log.Error("Failed to marshal notification: %v", err)
			continue
		}

		_, err = me.core.GetClient().XAdd(ctx, &redis.XAddArgs{
			Stream: me.core.GetKeyGen().GetNotificationChannelKey(config.ServiceId),
			Values: map[string]interface{}{
				"data": base64.StdEncoding.EncodeToString(notifBytes),
			},
			MaxLen: MaxStreamLength,
			Approx: true,
		}).Result()

		if err != nil {
			log.Error("Failed to publish notification: %v", err)
		}
	}
}

func (me *NotificationPublisher) decodeConfig(encoded string) *protobufs.DatabaseNotificationConfig {
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		log.Error("Failed to decode config: %v", err)
		return nil
	}

	config := &protobufs.DatabaseNotificationConfig{}
	if err := proto.Unmarshal(b, config); err != nil {
		log.Error("Failed to unmarshal config: %v", err)
		return nil
	}

	return config
}
