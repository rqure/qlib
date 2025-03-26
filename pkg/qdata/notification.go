package qdata

import "context"

type Notification interface {
	GetToken() string
	GetCurrent() *Field
	GetPrevious() *Field
	GetContext(index int) *Field
	GetContextCount() int
}

type NotificationConfig interface {
	GetEntityId() EntityId
	GetEntityType() EntityType
	GetFieldType() FieldType
	GetContextFields() []FieldType
	GetNotifyOnChange() bool
	GetServiceId() string
	GetToken() string
	IsDistributed() bool

	SetEntityId(EntityId) NotificationConfig
	SetEntityType(EntityType) NotificationConfig
	SetFieldType(FieldType) NotificationConfig
	SetContextFields(...FieldType) NotificationConfig
	SetNotifyOnChange(bool) NotificationConfig
	SetServiceId(string) NotificationConfig
	SetDistributed(bool) NotificationConfig
}

type NotificationCallback interface {
	Fn(context.Context, Notification)
	Id() string
}

type NotificationToken interface {
	Id() string
	Unbind(context.Context)
}
