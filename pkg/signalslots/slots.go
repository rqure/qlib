package signalslots

type Slot interface {
	Invoke(...interface{})
}

type SlotImpl struct {
	Fn func(...interface{})
}

func NewSlotWithArgs(fn func(...interface{})) Slot {
	return &SlotImpl{Fn: fn}
}

func NewSlot(fn func()) Slot {
	return NewSlotWithArgs(func(args ...interface{}) {
		fn()
	})
}

func (s *SlotImpl) Invoke(args ...interface{}) {
	s.Fn(args...)
}
