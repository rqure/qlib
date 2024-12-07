package data

type Store interface {
	Connect()
	Disconnect()
	IsConnected() bool

	CreateSnapshot() Snapshot
	RestoreSnapshot(s Snapshot)

	CreateEntity(entityType, parentId, name string)
	GetEntity(entityId string) Entity
	SetEntity(entityId string, value Entity)
	DeleteEntity(entityId string)

	FindEntities(entityType string) []string
	GetEntityTypes() []string

	EntityExists(entityId string) bool
	FieldExists(fieldName, entityType string) bool

	GetEntitySchema(entityType string) EntitySchema
	SetEntitySchema(entityType string, value EntitySchema)

	Read(requests []Request)
	Write(requests []Request)

	Notify(config NotificationConfig, callback NotificationCallback) NotificationToken
	Unnotify(subscriptionId string)
	UnnotifyCallback(subscriptionId string, callback NotificationCallback)
	ProcessNotifications()
}
