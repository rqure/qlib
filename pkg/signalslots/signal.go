package ss

type ISignal interface {
	Connect(ISlot)
	Disconnect(ISlot)
	DisconnectAll()
	Emit(...interface{})
}

type Signal struct {
	slots []ISlot
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
