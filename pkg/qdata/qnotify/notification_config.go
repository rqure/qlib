package qnotify

import (
	"encoding/base64"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qprotobufs"
	"google.golang.org/protobuf/proto"
)

func ToConfigPb(n qdata.NotificationConfig) *qprotobufs.DatabaseNotificationConfig {
	if n == nil {
		return nil
	}

	switch c := n.(type) {
	case *notificationConfig:
		return c.impl
	default:
		return nil
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

	return &notificationConfig{
		impl: impl,
	}
}

type notificationConfig struct {
	impl *qprotobufs.DatabaseNotificationConfig
}

func NewConfig() qdata.NotificationConfig {
	return &notificationConfig{
		impl: &qprotobufs.DatabaseNotificationConfig{},
	}
}

func FromConfigPb(impl *qprotobufs.DatabaseNotificationConfig) qdata.NotificationConfig {
	return &notificationConfig{
		impl: impl,
	}
}

func (me *notificationConfig) GetEntityId() string {
	return me.impl.Id
}

func (me *notificationConfig) GetEntityType() string {
	return me.impl.Type
}

func (me *notificationConfig) GetFieldName() string {
	return me.impl.Field
}

func (me *notificationConfig) GetContextFields() []string {
	if me.impl.ContextFields != nil {
		return me.impl.ContextFields
	}

	return []string{}
}

func (me *notificationConfig) GetNotifyOnChange() bool {
	return me.impl.NotifyOnChange
}

func (me *notificationConfig) GetServiceId() string {
	return me.impl.ServiceId
}

func (me *notificationConfig) GetToken() string {
	b, err := proto.Marshal(me.impl)
	if err != nil {
		qlog.Error("Failed to marshal notification config: %v", err)
		return ""
	}

	return base64.StdEncoding.EncodeToString(b)
}

func (me *notificationConfig) IsDistributed() bool {
	return me.impl.Distributed
}

func (me *notificationConfig) SetEntityId(id string) qdata.NotificationConfig {
	me.impl.Id = id
	return me
}

func (me *notificationConfig) SetEntityType(t string) qdata.NotificationConfig {
	me.impl.Type = t
	return me
}

func (me *notificationConfig) SetFieldName(f string) qdata.NotificationConfig {
	me.impl.Field = f
	return me
}

func (me *notificationConfig) SetContextFields(cf ...string) qdata.NotificationConfig {
	me.impl.ContextFields = cf
	return me
}

func (me *notificationConfig) SetNotifyOnChange(no bool) qdata.NotificationConfig {
	me.impl.NotifyOnChange = no
	return me
}

func (me *notificationConfig) SetServiceId(si string) qdata.NotificationConfig {
	me.impl.ServiceId = si
	return me
}

func (me *notificationConfig) SetDistributed(d bool) qdata.NotificationConfig {
	me.impl.Distributed = d
	return me
}
