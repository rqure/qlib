package qsignals

type ISlot interface {
	Invoke(...interface{})
}

type ISignal interface {
	Connect(ISlot)
	Disconnect(ISlot)
	DisconnectAll()
	Emit(...interface{})
}

type Signal struct {
	slots []ISlot
}

type SlotDefinition struct {
	Fn func(...interface{})
}

func (s *Signal) Connect(slot ISlot) {
	s.slots = append(s.slots, slot)
}

func (s *Signal) Disconnect(slot ISlot) {
	for i, v := range s.slots {
		if v == slot {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
}

func (s *Signal) Emit(args ...interface{}) {
	for _, slot := range s.slots {
		slot.Invoke(args...)
	}
}

func (s *Signal) DisconnectAll() {
	s.slots = []ISlot{}
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
