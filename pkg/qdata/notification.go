package qdata

import "context"

type Notification interface {
	GetToken() string
	GetCurrent() Field
	GetPrevious() Field
	GetContext(index int) Field
	GetContextCount() int
}

type NotificationConfig interface {
	GetEntityId() string
	GetEntityType() string
	GetFieldName() string
	GetContextFields() []string
	GetNotifyOnChange() bool
	GetServiceId() string
	GetToken() string
	IsDistributed() bool

	SetEntityId(string) NotificationConfig
	SetEntityType(string) NotificationConfig
	SetFieldName(string) NotificationConfig
	SetContextFields(...string) NotificationConfig
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
