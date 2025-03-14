package qnotify

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
)

type Token struct {
	tokenId  string
	manager  qdata.NotificationConsumer
	callback qdata.NotificationCallback
}

func NewToken(tokenId string, store qdata.NotificationConsumer, callback qdata.NotificationCallback) qdata.NotificationToken {
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
