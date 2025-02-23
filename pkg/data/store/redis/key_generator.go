package redis

type KeyGenerator interface {
	GetEntitySchemaKey(entityType string) string
	GetEntityKey(entityId string) string
	GetFieldKey(fieldName, entityId string) string
	GetEntityTypeKey(entityType string) string
	GetEntityIdNotificationConfigKey(entityId, fieldName string) string
	GetEntityTypeNotificationConfigKey(entityType, fieldName string) string
	GetNotificationChannelKey(serviceId string) string
}

type keyGenerator struct{}

func NewKeyGenerator() KeyGenerator {
	return &keyGenerator{}
}

func (g *keyGenerator) GetEntitySchemaKey(entityType string) string {
	return "schema:entity:" + entityType
}

func (g *keyGenerator) GetEntityKey(entityId string) string {
	return "instance:entity:" + entityId
}

func (g *keyGenerator) GetFieldKey(fieldName, entityId string) string {
	return "instance:field:" + fieldName + ":" + entityId
}

func (g *keyGenerator) GetEntityTypeKey(entityType string) string {
	return "instance:type:" + entityType
}

func (g *keyGenerator) GetEntityIdNotificationConfigKey(entityId, fieldName string) string {
	return "instance:notification-config:" + entityId + ":" + fieldName
}

func (g *keyGenerator) GetEntityTypeNotificationConfigKey(entityType, fieldName string) string {
	return "instance:notification-config:" + entityType + ":" + fieldName
}

func (g *keyGenerator) GetNotificationChannelKey(serviceId string) string {
	return "instance:notification:" + serviceId
}
