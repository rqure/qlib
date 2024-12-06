package notification

import (
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/protobufs"
)

type NotificationWrapper struct {
	impl *protobufs.DatabaseNotification
}

func FromPb(impl *protobufs.DatabaseNotification) data.Notification {
	return &NotificationWrapper{
		impl: impl,
	}
}

func (n *NotificationWrapper) GetToken() string {
	return n.impl.Token
}

func (n *NotificationWrapper) GetCurrent() data.Field {
	return field.FromPb(n.impl.Current)
}

func (n *NotificationWrapper) GetPrevious() data.Field {
	return field.FromPb(n.impl.Previous)
}

func (n *NotificationWrapper) GetContext(index int) data.Field {
	return field.FromPb(n.impl.Context[index])
}

type ConfigWrapper struct {
	impl *protobufs.DatabaseNotificationConfig
}

func FromConfigPb(impl *protobufs.DatabaseNotificationConfig) data.NotificationConfig {
	return &ConfigWrapper{
		impl: impl,
	}
}

func (c *ConfigWrapper) GetEntityId() string {
	return c.impl.Id
}

func (c *ConfigWrapper) GetEntityType() string {
	return c.impl.Type
}

func (c *ConfigWrapper) GetField() string {
	return c.impl.Field
}

func (c *ConfigWrapper) GetContextFields() []string {
	return c.impl.ContextFields
}

func (c *ConfigWrapper) GetNotifyOnChange() bool {
	return c.impl.NotifyOnChange
}

func (c *ConfigWrapper) GetServiceId() string {
	return c.impl.ServiceId
}

func (c *ConfigWrapper) SetEntityId(id string) data.NotificationConfig {
	c.impl.Id = id
	return c
}

func (c *ConfigWrapper) SetEntityType(t string) data.NotificationConfig {
	c.impl.Type = t
	return c
}

func (c *ConfigWrapper) SetField(f string) data.NotificationConfig {
	c.impl.Field = f
	return c
}

func (c *ConfigWrapper) SetContextFields(cf []string) data.NotificationConfig {
	c.impl.ContextFields = cf
	return c
}

func (c *ConfigWrapper) SetNotifyOnChange(no bool) data.NotificationConfig {
	c.impl.NotifyOnChange = no
	return c
}

func (c *ConfigWrapper) SetServiceId(si string) data.NotificationConfig {
	c.impl.ServiceId = si
	return c
}
