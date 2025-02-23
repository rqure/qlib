package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

const (
	NotificationExpiryDuration = time.Minute
)

type NotificationConsumer struct {
	core        Core
	transformer data.Transformer

	callbacks map[string][]data.NotificationCallback
}

func NewNotificationManager(core Core) data.NotificationConsumer {
	return &NotificationConsumer{
		core:      core,
		callbacks: map[string][]data.NotificationCallback{},
	}
}

func (me *NotificationConsumer) SetTransformer(transformer data.Transformer) {
	me.transformer = transformer
}

// Fix notification processing to avoid lock copying
func (me *NotificationConsumer) ProcessNotifications(ctx context.Context) {
	me.transformer.ProcessPending()

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		expireTime := time.Now().Add(-NotificationExpiryDuration)
		_, err := tx.Exec(ctx, `
			DELETE FROM Notifications
			WHERE timestamp < $1
		`, expireTime)
		if err != nil {
			log.Error("Failed to delete expired notifications: %v", err)
			return
		}

		// Select and delete notifications in one transaction to prevent duplicates
		rows, err := tx.Query(ctx, `
			DELETE FROM Notifications 
			WHERE service_id = $1 
			AND timestamp > $2
			RETURNING notification
		`, me.getServiceId(), expireTime)

		if err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to process notifications: %v", err)
			}
			return
		}
		defer rows.Close()

		for rows.Next() {
			var notifBytes []byte
			err := rows.Scan(&notifBytes)
			if err != nil {
				log.Error("Failed to scan notification: %v", err)
				continue
			}

			n := &protobufs.DatabaseNotification{}
			if err := proto.Unmarshal(notifBytes, n); err != nil {
				log.Error("Failed to unmarshal notification: %v", err)
				continue
			}

			if callbacks, ok := me.callbacks[n.Token]; ok {
				notif := notification.FromPb(n)
				for _, callback := range callbacks {
					callback.Fn(ctx, notif)
				}
			}
		}
	})
}

func (me *NotificationConsumer) Notify(ctx context.Context, nc data.NotificationConfig, cb data.NotificationCallback) data.NotificationToken {
	if nc.GetServiceId() == "" {
		nc.SetServiceId(me.getServiceId())
	}

	token := nc.GetToken()

	var n data.NotificationToken
	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		if nc.GetEntityId() != "" {
			_, err := tx.Exec(ctx, `
				INSERT INTO NotificationConfigEntityId (entity_id, field_name, context_fields, notify_on_change, service_id, token)
				VALUES ($1, $2, $3, $4, $5, $6)
			`, nc.GetEntityId(), nc.GetFieldName(), nc.GetContextFields(), nc.GetNotifyOnChange(), nc.GetServiceId(), token)

			if err != nil {
				log.Error("Failed to create notification config: %v", err)
				return
			}
		} else {
			_, err := tx.Exec(ctx, `
				INSERT INTO NotificationConfigEntityType (entity_type, field_name, context_fields, notify_on_change, service_id, token)
				VALUES ($1, $2, $3, $4, $5, $6)
			`, nc.GetEntityType(), nc.GetFieldName(), nc.GetContextFields(), nc.GetNotifyOnChange(), nc.GetServiceId(), token)

			if err != nil {
				log.Error("Failed to create notification config: %v", err)
				return
			}
		}

		me.callbacks[token] = append(me.callbacks[token], cb)
		n = notification.NewToken(token, me, cb)
	})

	if n == nil {
		n = notification.NewToken("", me, nil)
	}

	return n
}

func (me *NotificationConsumer) getServiceId() string {
	return app.GetName()
}

func (me *NotificationConsumer) Unnotify(ctx context.Context, token string) {
	nc := notification.FromToken(token)

	if nc == nil {
		log.Error("Invalid notification token: %s", token)
		return
	}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		var err error

		// Delete based on service_id and other matching fields
		if nc.GetEntityId() != "" {
			_, err = tx.Exec(ctx, `
			DELETE FROM NotificationConfigEntityId 
			WHERE service_id = $1 
			AND entity_id = $2 
			AND field_name = $3
			`, nc.GetServiceId(), nc.GetEntityId(), nc.GetFieldName())
		} else {
			_, err = tx.Exec(ctx, `
			DELETE FROM NotificationConfigEntityType 
			WHERE service_id = $1 
			AND entity_type = $2 
			AND field_name = $3
			`, nc.GetServiceId(), nc.GetEntityType(), nc.GetFieldName())
		}

		if err != nil {
			log.Error("Failed to delete notification config: %v", err)
			return
		}

		delete(me.callbacks, token)
	})
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
