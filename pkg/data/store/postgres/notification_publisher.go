package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
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
	return &NotificationPublisher{
		core: core,
	}
}

func (me *NotificationPublisher) SetEntityManager(entityManager data.EntityManager) {
	me.entityManager = entityManager
}

func (me *NotificationPublisher) SetFieldOperator(fieldOperator data.FieldOperator) {
	me.fieldOperator = fieldOperator
}

func (me *NotificationPublisher) processNotificationRows(ctx context.Context, rows pgx.Rows, r data.Request, o data.Request) []*protobufs.DatabaseNotification {
	notifications := []*protobufs.DatabaseNotification{}

	for rows.Next() {
		var id int
		var contextFields []string
		var notifyOnChange bool
		var serviceId string
		var token string

		err := rows.Scan(&id, &contextFields, &notifyOnChange, &serviceId, &token)
		if err != nil {
			log.Error("Failed to scan notification config: %v", err)
			continue
		}

		// Create context fields
		context := []*protobufs.DatabaseField{}
		for _, cf := range contextFields {
			cr := request.New().SetEntityId(r.GetEntityId()).SetFieldName(cf)
			me.fieldOperator.Read(ctx, cr)
			if cr.IsSuccessful() {
				context = append(context, field.ToFieldPb(field.FromRequest(cr)))
			}
		}

		notifications = append(notifications, &protobufs.DatabaseNotification{
			Token:     token,
			ServiceId: serviceId,
			Current:   field.ToFieldPb(field.FromRequest(r)),
			Previous:  field.ToFieldPb(field.FromRequest(o)),
			Context:   context,
		})
	}

	return notifications
}

func (me *NotificationPublisher) TriggerNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	notifications := []*protobufs.DatabaseNotification{}

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		rows, err := tx.Query(ctx, `
		SELECT id, context_fields, notify_on_change, service_id, token
		FROM NotificationConfigEntityId
		WHERE entity_id = $1 AND field_name = $2
	`, curr.GetEntityId(), curr.GetFieldName())
		if err != nil {
			log.Error("Failed to get entity notifications: %v", err)
			return
		}
		defer rows.Close()

		notifications = append(notifications, me.processNotificationRows(ctx, rows, curr, prev)...)
	})

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		entity := me.entityManager.GetEntity(ctx, curr.GetEntityId())
		if entity == nil {
			log.Error("Failed to get entity")
			return
		}

		rows, err := tx.Query(ctx, `
			SELECT id, context_fields, notify_on_change, service_id, token
			FROM NotificationConfigEntityType
			WHERE entity_type = $1 AND field_name = $2
		`, entity.GetType(), curr.GetFieldName())
		if err != nil {
			log.Error("Failed to get type notifications: %v", err)
			return
		}
		defer rows.Close()

		notifications = append(notifications, me.processNotificationRows(ctx, rows, curr, prev)...)
	})

	me.core.WithTx(ctx, func(ctx context.Context, tx pgx.Tx) {
		for _, n := range notifications {
			notifBytes, err := proto.Marshal(n)
			if err != nil {
				log.Error("Failed to marshal notification: %v", err)
				continue
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO Notifications (timestamp, service_id, notification)
				VALUES ($1, $2, $3)
			`, time.Now(), n.ServiceId, notifBytes)
			if err != nil {
				log.Error("Failed to insert notification: %v", err)
			}
		}
	})
}
