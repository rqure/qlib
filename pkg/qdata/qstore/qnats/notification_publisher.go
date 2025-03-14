package qnats

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
)

type NotificationPublisher struct {
	core          Core
	entityManager qdata.EntityManager
	fieldOperator qdata.FieldOperator
}

func NewNotificationPublisher(core Core) qdata.ModifiableNotificationPublisher {
	return &NotificationPublisher{core: core}
}

func (p *NotificationPublisher) SetEntityManager(em qdata.EntityManager) {
	p.entityManager = em
}

func (p *NotificationPublisher) SetFieldOperator(fo qdata.FieldOperator) {
	p.fieldOperator = fo
}

func (p *NotificationPublisher) PublishNotifications(ctx context.Context, curr qdata.Request, prev qdata.Request) {

}
