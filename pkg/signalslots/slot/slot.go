package slot

import (
	"context"

	"github.com/rqure/qlib/pkg/signalslots"
)

type SlotWithArgs func(...interface{})

type SlotWithoutArgs func()

type SlotWithContext func(context.Context)

type SlotWithContextAndArgs func(context.Context, ...interface{})

type SlotWrapper struct {
	Fn func(...interface{})
}

func New(fn func(...interface{})) signalslots.Slot {
	return &SlotWrapper{Fn: fn}
}

func (s *SlotWrapper) Invoke(args ...interface{}) {
	s.Fn(args...)
}

func (s SlotWithArgs) Invoke(args ...interface{}) {
	s(args...)
}

func (s SlotWithoutArgs) Invoke(args ...interface{}) {
	s()
}

func (s SlotWithContext) Invoke(args ...interface{}) {
	s(args[0].(context.Context))
}

func (s SlotWithContextAndArgs) Invoke(args ...interface{}) {
	s(args[0].(context.Context), args[1:]...)
}
