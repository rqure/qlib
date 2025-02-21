package notification

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
)

type Token struct {
	subscriptionId string
	manager        data.NotificationManager
	callback       data.NotificationCallback
}

func NewToken(subscriptionId string, store data.NotificationManager, callback data.NotificationCallback) data.NotificationToken {
	return &Token{
		subscriptionId: subscriptionId,
		manager:        store,
		callback:       callback,
	}
}

func (t *Token) Id() string {
	return t.subscriptionId
}

func (t *Token) Unbind(ctx context.Context) {
	if t.callback != nil {
		t.manager.UnnotifyCallback(ctx, t.subscriptionId, t.callback)
	} else {
		t.manager.Unnotify(ctx, t.subscriptionId)
	}
}
