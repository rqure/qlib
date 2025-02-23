package web

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
)

type NotificationPublisher struct {
	core          Core
	entityManager data.EntityManager
	fieldOperator data.FieldOperator
}

func NewNotificationPublisher(core Core) data.ModifiableNotificationPublisher {
	return &NotificationPublisher{core: core}
}

func (p *NotificationPublisher) SetEntityManager(em data.EntityManager) {
	p.entityManager = em
}

func (p *NotificationPublisher) SetFieldOperator(fo data.FieldOperator) {
	p.fieldOperator = fo
}

func (p *NotificationPublisher) PublishNotifications(ctx context.Context, curr data.Request, prev data.Request) {
	// Web implementation doesn't need to publish notifications
	// since the server handles this internally
}
