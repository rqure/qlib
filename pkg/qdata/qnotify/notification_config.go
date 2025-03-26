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

func (me *notificationConfig) GetEntityId() qdata.EntityId {
	return qdata.EntityId(me.impl.Id)
}

func (me *notificationConfig) GetEntityType() qdata.EntityType {
	return qdata.EntityType(me.impl.Type)
}

func (me *notificationConfig) GetFieldType() qdata.FieldType {
	return qdata.FieldType(me.impl.Field)
}

func (me *notificationConfig) GetContextFields() []qdata.FieldType {
	if me.impl.ContextFields != nil {
		fields := make([]qdata.FieldType, len(me.impl.ContextFields))
		for i, f := range me.impl.ContextFields {
			fields[i] = qdata.FieldType(f)
		}

		return fields
	}

	return []qdata.FieldType{}
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

func (me *notificationConfig) SetEntityId(id qdata.EntityId) qdata.NotificationConfig {
	me.impl.Id = id.AsString()
	return me
}

func (me *notificationConfig) SetEntityType(t qdata.EntityType) qdata.NotificationConfig {
	me.impl.Type = t.AsString()
	return me
}

func (me *notificationConfig) SetFieldType(f qdata.FieldType) qdata.NotificationConfig {
	me.impl.Field = f.AsString()
	return me
}

func (me *notificationConfig) SetContextFields(cf ...qdata.FieldType) qdata.NotificationConfig {
	me.impl.ContextFields = make([]string, len(cf))
	for i, f := range cf {
		me.impl.ContextFields[i] = f.AsString()
	}
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
