package qnotify

import (
	"encoding/base64"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qfield"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

type NotificationWrapper struct {
	impl *qprotobufs.DatabaseNotification
}

func ToPb(n qdata.Notification) *qprotobufs.DatabaseNotification {
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

func ToConfigPb(n qdata.NotificationConfig) *qprotobufs.DatabaseNotificationConfig {
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

func FromPb(impl *qprotobufs.DatabaseNotification) qdata.Notification {
	return &NotificationWrapper{
		impl: impl,
	}
}

func FromToken(token string) qdata.NotificationConfig {
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		qlog.Error("Failed to decode notification token: %v", err)
		return nil
	}

	impl := &qprotobufs.DatabaseNotificationConfig{}
	if err := proto.Unmarshal(b, impl); err != nil {
		qlog.Error("Failed to unmarshal notification token: %v", err)
		return nil
	}

	return &ConfigWrapper{
		impl: impl,
	}
}

func (n *NotificationWrapper) GetToken() string {
	return n.impl.Token
}

func (n *NotificationWrapper) GetCurrent() qdata.Field {
	return qfield.FromFieldPb(n.impl.Current)
}

func (n *NotificationWrapper) GetPrevious() qdata.Field {
	return qfield.FromFieldPb(n.impl.Previous)
}

func (n *NotificationWrapper) GetContext(index int) qdata.Field {
	return qfield.FromFieldPb(n.impl.Context[index])
}

func (n *NotificationWrapper) GetContextCount() int {
	return len(n.impl.Context)
}

type ConfigWrapper struct {
	impl *qprotobufs.DatabaseNotificationConfig
}

func NewConfig() qdata.NotificationConfig {
	return &ConfigWrapper{
		impl: &qprotobufs.DatabaseNotificationConfig{},
	}
}

func FromConfigPb(impl *qprotobufs.DatabaseNotificationConfig) qdata.NotificationConfig {
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
		qlog.Error("Failed to marshal notification config: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(b)
}

func (me *ConfigWrapper) IsDistributed() bool {
	return me.impl.Distributed
}

func (me *ConfigWrapper) SetEntityId(id string) qdata.NotificationConfig {
	me.impl.Id = id
	return me
}

func (me *ConfigWrapper) SetEntityType(t string) qdata.NotificationConfig {
	me.impl.Type = t
	return me
}

func (me *ConfigWrapper) SetFieldName(f string) qdata.NotificationConfig {
	me.impl.Field = f
	return me
}

func (me *ConfigWrapper) SetContextFields(cf ...string) qdata.NotificationConfig {
	me.impl.ContextFields = cf
	return me
}

func (me *ConfigWrapper) SetNotifyOnChange(no bool) qdata.NotificationConfig {
	me.impl.NotifyOnChange = no
	return me
}

func (me *ConfigWrapper) SetServiceId(si string) qdata.NotificationConfig {
	me.impl.ServiceId = si
	return me
}

func (me *ConfigWrapper) SetDistributed(d bool) qdata.NotificationConfig {
	me.impl.Distributed = d
	return me
}
