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

func (me *ConfigWrapper) GetEntityId() string {
	return me.impl.Id
}

func (me *ConfigWrapper) GetEntityType() string {
	return me.impl.Type
}

func (me *ConfigWrapper) GetFieldName() string {
	return me.impl.Field
}

func (me *ConfigWrapper) GetContextFields() []string {
	if me.impl.ContextFields != nil {
		return me.impl.ContextFields
	}

	return []string{}
}

func (me *ConfigWrapper) GetNotifyOnChange() bool {
	return me.impl.NotifyOnChange
}

func (me *ConfigWrapper) GetServiceId() string {
	return me.impl.ServiceId
}

func (me *ConfigWrapper) GetToken() string {
	b, err := proto.Marshal(me.impl)
	if err != nil {
		log.Error("Failed to marshal notification config: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(b)
}

func (me *ConfigWrapper) IsDistributed() bool {
	return me.impl.Distributed
}

func (me *ConfigWrapper) SetEntityId(id string) data.NotificationConfig {
	me.impl.Id = id
	return me
}

func (me *ConfigWrapper) SetEntityType(t string) data.NotificationConfig {
	me.impl.Type = t
	return me
}

func (me *ConfigWrapper) SetFieldName(f string) data.NotificationConfig {
	me.impl.Field = f
	return me
}

func (me *ConfigWrapper) SetContextFields(cf ...string) data.NotificationConfig {
	me.impl.ContextFields = cf
	return me
}

func (me *ConfigWrapper) SetNotifyOnChange(no bool) data.NotificationConfig {
	me.impl.NotifyOnChange = no
	return me
}

func (me *ConfigWrapper) SetServiceId(si string) data.NotificationConfig {
	me.impl.ServiceId = si
	return me
}

func (me *ConfigWrapper) SetDistributed(d bool) data.NotificationConfig {
	me.impl.Distributed = d
	return me
}
