package data

import (
	pb "github.com/rqure/qlib/pkg/protobufs"
)

type INotificationCallback interface {
	Fn(*pb.DatabaseNotification)
	Id() string
}

type INotificationToken interface {
	Id() string
	Unbind()
}

type NotificationToken struct {
	db             IDatabase
	subscriptionId string
	callback       INotificationCallback
}

func (t *NotificationToken) Id() string {
	return t.subscriptionId
}

func (t *NotificationToken) Unbind() {
	if t.callback != nil {
		t.db.UnnotifyCallback(t.subscriptionId, t.callback)
	} else {
		t.db.Unnotify(t.subscriptionId)
	}
}
