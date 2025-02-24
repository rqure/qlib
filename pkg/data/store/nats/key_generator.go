package nats

type KeyGenerator interface {
	GetReadSubject() string
	GetWriteSubject() string
	GetNotificationSubject() string
	GetNotificationGroupSubject(serviceId string) string
}

type keyGenerator struct{}

func NewKeyGenerator() KeyGenerator {
	return &keyGenerator{}
}

func (g *keyGenerator) GetReadSubject() string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetWriteSubject() string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetNotificationSubject() string {
	return "q.store.notification"
}

func (g *keyGenerator) GetNotificationGroupSubject(serviceId string) string {
	return "q.store.notification.group." + serviceId
}
