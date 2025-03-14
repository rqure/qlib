package qnotify

import (
	"context"

	"github.com/google/uuid"
	"github.com/rqure/qlib/pkg/qdata"
)

type Callback struct {
	fn func(context.Context, qdata.Notification)
	id string
}

func NewCallback(fn func(context.Context, qdata.Notification)) qdata.NotificationCallback {
	return &Callback{
		fn: fn,
		id: uuid.New().String(),
	}
}

func (c *Callback) Fn(ctx context.Context, n qdata.Notification) {
	c.fn(ctx, n)
}

func (c *Callback) Id() string {
	return c.id
}
