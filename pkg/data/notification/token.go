package notification

import "github.com/rqure/qlib/pkg/data"

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

func (t *Token) Unbind() {
	if t.callback != nil {
		t.store.UnnotifyCallback(t.subscriptionId, t.callback)
	} else {
		t.store.Unnotify(t.subscriptionId)
	}
}
