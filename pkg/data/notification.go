package data

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
	GetField() string
	GetContextFields() []string
	GetNotifyOnChange() bool
	GetServiceId() string

	SetEntityId(string) NotificationConfig
	SetEntityType(string) NotificationConfig
	SetField(string) NotificationConfig
	SetContextFields([]string) NotificationConfig
	SetNotifyOnChange(bool) NotificationConfig
	SetServiceId(string) NotificationConfig
}

type NotificationCallback interface {
	Fn(Notification)
	Id() string
}

type NotificationToken interface {
	Id() string
	Unbind()
}
