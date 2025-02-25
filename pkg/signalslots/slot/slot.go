package slot

import (
	"context"

	"github.com/rqure/qlib/pkg/signalslots"
)

type SlotWithArgs func(...interface{})

type SlotWithoutArgs func()

type SlotWithContext func(context.Context)

type SlotWithContextAndArgs func(context.Context, ...interface{})

type SlotWithContextErrorAndArgs func(context.Context, error, ...interface{})

type SlotWithContextError func(context.Context, error)

type SlotWithError func(error)

type SlotWithErrorAndArgs func(error, ...interface{})

type SlotWithCallback func(func(context.Context))

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

func (s SlotWithContextErrorAndArgs) Invoke(args ...interface{}) {
	s(args[0].(context.Context), args[1].(error), args[2:]...)
}

func (s SlotWithError) Invoke(args ...interface{}) {
	s(args[0].(error))
}

func (s SlotWithErrorAndArgs) Invoke(args ...interface{}) {
	s(args[0].(error), args[1:]...)
}

func (s SlotWithContextError) Invoke(args ...interface{}) {
	s(args[0].(context.Context), args[1].(error))
}

func (s SlotWithCallback) Invoke(args ...interface{}) {
	s(args[0].(func(context.Context)))
}
