package notification

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
)

type Token struct {
	subscriptionId string
	store          data.Store
	callback       data.NotificationCallback
}

func NewToken(subscriptionId string, store data.Store, callback data.NotificationCallback) data.NotificationToken {
	return &Token{
		subscriptionId: subscriptionId,
		store:          store,
		callback:       callback,
	}
}

func (t *Token) Id() string {
	return t.subscriptionId
}

func (t *Token) Unbind(ctx context.Context) {
	if t.callback != nil {
		t.store.UnnotifyCallback(ctx, t.subscriptionId, t.callback)
	} else {
		t.store.Unnotify(ctx, t.subscriptionId)
	}
}
