package qnotify

import (
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qprotobufs"
)

type notification struct {
	impl *qprotobufs.DatabaseNotification
}

func ToPb(n qdata.Notification) *qprotobufs.DatabaseNotification {
	if n == nil {
		return nil
	}

	switch c := n.(type) {
	case *notification:
		return c.impl
	default:
		return nil
	}
}

func FromPb(impl *qprotobufs.DatabaseNotification) qdata.Notification {
	return &notification{
		impl: impl,
	}
}

func (me *notification) GetToken() string {
	return me.impl.Token
}

func (me *notification) GetCurrent() *qdata.Field {
	return new(qdata.Field).FromFieldPb(me.impl.Current)
}

func (me *notification) GetPrevious() *qdata.Field {
	return new(qdata.Field).FromFieldPb(me.impl.Previous)
}

func (me *notification) GetContext(index int) *qdata.Field {
	return new(qdata.Field).FromFieldPb(me.impl.Context[index])
}

func (me *notification) GetContextCount() int {
	return len(me.impl.Context)
}
