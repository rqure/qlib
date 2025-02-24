package nats

type KeyGenerator interface {
	GetEntitySchemaSubject(entityType string) string
	GetEntitySubject(entityId string) string
	GetFieldSubject(fieldName, entityId string) string
	GetEntityTypeSubject(entityType string) string
	GetNotificationSubject() string
	GetNotificationRegisterSubject() string
	GetNotificationUnregisterSubject() string
	GetEntityExistsSubject() string
	GetFieldExistsSubject() string
	GetSnapshotCreateSubject() string
	GetSnapshotRestoreSubject() string
	GetNotificationQueueGroup(serviceId string) string
}

type keyGenerator struct{}

func NewKeyGenerator() KeyGenerator {
	return &keyGenerator{}
}

func (g *keyGenerator) GetEntitySchemaSubject(entityType string) string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetEntitySubject(entityId string) string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetFieldSubject(fieldName, entityId string) string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetEntityTypeSubject(entityType string) string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetNotificationSubject() string {
	return "q.store.notification.request"
}

func (g *keyGenerator) GetNotificationRegisterSubject() string {
	return "q.store.notification.request"
}

func (g *keyGenerator) GetNotificationUnregisterSubject() string {
	return "q.store.notification.request"
}

func (g *keyGenerator) GetEntityExistsSubject() string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetFieldExistsSubject() string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetSnapshotCreateSubject() string {
	return "q.store.read.request"
}

func (g *keyGenerator) GetSnapshotRestoreSubject() string {
	return "q.store.write.request"
}

func (g *keyGenerator) GetNotificationQueueGroup(serviceId string) string {
	return "q.store.notification.event." + serviceId
}
