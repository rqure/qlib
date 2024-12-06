package ss

type ISlot interface {
	Invoke(...interface{})
}

type SlotDefinition struct {
	Fn func(...interface{})
}

func SlotWithArgs(fn func(...interface{})) ISlot {
	return &SlotDefinition{Fn: fn}
}

func Slot(fn func()) ISlot {
	return SlotWithArgs(func(args ...interface{}) {
		fn()
	})
}

func (s *SlotDefinition) Invoke(args ...interface{}) {
	s.Fn(args...)
}
