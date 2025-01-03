package notification

import (
	"encoding/base64"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/field"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/protobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationWrapper struct {
	impl *protobufs.DatabaseNotification
}

func ToPb(n data.Notification) *protobufs.DatabaseNotification {
	if n == nil {
		return nil
	}

	switch c := n.(type) {
	case *NotificationWrapper:
		return c.impl
	default:
		return nil
	}
}

func ToConfigPb(n data.NotificationConfig) *protobufs.DatabaseNotificationConfig {
	if n == nil {
		return nil
	}

	switch c := n.(type) {
	case *ConfigWrapper:
		return c.impl
	default:
		return nil
	}
}

func FromPb(impl *protobufs.DatabaseNotification) data.Notification {
	return &NotificationWrapper{
		impl: impl,
	}
}

func FromToken(token string) data.NotificationConfig {
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		log.Error("Failed to decode notification token: %v", err)
		return nil
	}

	impl := &protobufs.DatabaseNotificationConfig{}
	if err := proto.Unmarshal(b, impl); err != nil {
		log.Error("Failed to unmarshal notification token: %v", err)
		return nil
	}

	return &ConfigWrapper{
		impl: impl,
	}
}

func (n *NotificationWrapper) GetToken() string {
	return n.impl.Token
}

func (n *NotificationWrapper) GetCurrent() data.Field {
	return field.FromFieldPb(n.impl.Current)
}

func (n *NotificationWrapper) GetPrevious() data.Field {
	return field.FromFieldPb(n.impl.Previous)
}

func (n *NotificationWrapper) GetContext(index int) data.Field {
	return field.FromFieldPb(n.impl.Context[index])
}

func (n *NotificationWrapper) GetContextCount() int {
	return len(n.impl.Context)
}

type ConfigWrapper struct {
	impl *protobufs.DatabaseNotificationConfig
}

func NewConfig() data.NotificationConfig {
	return &ConfigWrapper{
		impl: &protobufs.DatabaseNotificationConfig{},
	}
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

func (c *ConfigWrapper) GetFieldName() string {
	return c.impl.Field
}

func (c *ConfigWrapper) GetContextFields() []string {
	if c.impl.ContextFields != nil {
		return c.impl.ContextFields
	}

	return []string{}
}

func (c *ConfigWrapper) GetNotifyOnChange() bool {
	return c.impl.NotifyOnChange
}

func (c *ConfigWrapper) GetServiceId() string {
	return c.impl.ServiceId
}

func (c *ConfigWrapper) GetToken() string {
	b, err := proto.Marshal(c.impl)
	if err != nil {
		log.Error("Failed to marshal notification config: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(b)
}

func (c *ConfigWrapper) SetEntityId(id string) data.NotificationConfig {
	c.impl.Id = id
	return c
}

func (c *ConfigWrapper) SetEntityType(t string) data.NotificationConfig {
	c.impl.Type = t
	return c
}

func (c *ConfigWrapper) SetFieldName(f string) data.NotificationConfig {
	c.impl.Field = f
	return c
}

func (c *ConfigWrapper) SetContextFields(cf ...string) data.NotificationConfig {
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
