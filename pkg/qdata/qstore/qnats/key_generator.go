package qnats

type NatsKeyGenerator interface {
	GetReadSubject() string
	GetWriteSubject() string
	GetNotificationRegistrationSubject() string
	GetNotificationGroupSubject(serviceId string) string
	GetDistributedNotificationGroupSubject(serviceId string) string
}

type natsKeyGenerator struct{}

func NewKeyGenerator() NatsKeyGenerator {
	return &natsKeyGenerator{}
}

func (g *natsKeyGenerator) GetReadSubject() string {
	return "q.store.read.request"
}

func (g *natsKeyGenerator) GetWriteSubject() string {
	return "q.store.write.request"
}

func (g *natsKeyGenerator) GetNotificationRegistrationSubject() string {
	return "q.store.notification"
}

func (g *natsKeyGenerator) GetNotificationGroupSubject(serviceId string) string {
	return "q.store.notification.group." + serviceId
}

func (g *natsKeyGenerator) GetDistributedNotificationGroupSubject(serviceId string) string {
	return "q.store.notification.distributed." + serviceId
}
