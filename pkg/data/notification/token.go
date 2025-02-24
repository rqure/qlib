package notification

import (
	"context"

	"github.com/rqure/qlib/pkg/data"
)

type Token struct {
	tokenId  string
	manager  data.NotificationConsumer
	callback data.NotificationCallback
}

func NewToken(tokenId string, store data.NotificationConsumer, callback data.NotificationCallback) data.NotificationToken {
	return &Token{
		tokenId:  tokenId,
		manager:  store,
		callback: callback,
	}
}

func (t *Token) Id() string {
	return t.tokenId
}

func (t *Token) Unbind(ctx context.Context) {
	if t.callback != nil {
		t.manager.UnnotifyCallback(ctx, t.tokenId, t.callback)
	} else {
		t.manager.Unnotify(ctx, t.tokenId)
	}
}
