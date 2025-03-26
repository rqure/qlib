package qnotify

import (
	"context"

	"github.com/rqure/qlib/pkg/qdata"
)

type Token struct {
	tokenId       string
	storeNotifier qdata.StoreNotifier
	callback      qdata.NotificationCallback
}

func NewToken(tokenId string, storeNotifier qdata.StoreNotifier, callback qdata.NotificationCallback) qdata.NotificationToken {
	return &Token{
		tokenId:       tokenId,
		storeNotifier: storeNotifier,
		callback:      callback,
	}
}

func (t *Token) Id() string {
	return t.tokenId
}

func (t *Token) Unbind(ctx context.Context) {
	if t.callback != nil {
		t.storeNotifier.UnnotifyCallback(ctx, t.tokenId, t.callback)
	} else {
		t.storeNotifier.Unnotify(ctx, t.tokenId)
	}
}
