package signalslots

type Signal interface {
	Connect(Slot)
	Disconnect(Slot)
	DisconnectAll()
	Emit(...interface{})
}

type SignalImpl struct {
	slots []Slot
}

func NewSignal() Signal {
	return &SignalImpl{}
}

func (s *SignalImpl) Connect(slot Slot) {
	s.slots = append(s.slots, slot)
}

func (s *SignalImpl) Disconnect(slot Slot) {
	for i, v := range s.slots {
		if v == slot {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
}

func (s *SignalImpl) Emit(args ...interface{}) {
	for _, slot := range s.slots {
		slot.Invoke(args...)
	}
}

func (s *SignalImpl) DisconnectAll() {
	s.slots = []Slot{}
}
