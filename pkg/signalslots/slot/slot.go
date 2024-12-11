package slot

import "github.com/rqure/qlib/pkg/signalslots"

type SlotWithArgs func(...interface{})

type SlotWithoutArgs func()

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
