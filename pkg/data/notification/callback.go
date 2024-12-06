package notification

import (
	"github.com/google/uuid"
	"github.com/rqure/qlib/pkg/data"
)

type Callback struct {
	fn func(data.Notification)
	id string
}

func NewCallback(fn func(data.Notification)) data.NotificationCallback {
	return &Callback{
		fn: fn,
		id: uuid.New().String(),
	}
}

func (c *Callback) Fn(n data.Notification) {
	c.fn(n)
}

func (c *Callback) Id() string {
	return c.id
}
