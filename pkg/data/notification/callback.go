package notification

import (
	"context"

	"github.com/google/uuid"
	"github.com/rqure/qlib/pkg/data"
)

type Callback struct {
	fn func(context.Context, data.Notification)
	id string
}

func NewCallback(fn func(context.Context, data.Notification)) data.NotificationCallback {
	return &Callback{
		fn: fn,
		id: uuid.New().String(),
	}
}

func (c *Callback) Fn(ctx context.Context, n data.Notification) {
	c.fn(ctx, n)
}

func (c *Callback) Id() string {
	return c.id
}
