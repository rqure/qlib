package notification

import "github.com/google/uuid"

type Callback struct {
	fn func(*pb.DatabaseNotification)
	id string
}

func NewCallback(fn func(*pb.DatabaseNotification)) Callback {
	return &Callback{
		fn: fn,
		id: uuid.New().String(),
	}
}

func (c *Callback) Fn(n *pb.DatabaseNotification) {
	c.fn(n)
}

func (c *Callback) Id() string {
	return c.id
}
