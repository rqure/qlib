package signal

import "github.com/rqure/qlib/pkg/signalslots"

type SignalImpl struct {
	slots []signalslots.Slot
}

func NewSignal() signalslots.Signal {
	return &SignalImpl{}
}

func (s *SignalImpl) Connect(slot signalslots.Slot) {
	s.slots = append(s.slots, slot)
}

func (s *SignalImpl) Disconnect(slot signalslots.Slot) {
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
	s.slots = []signalslots.Slot{}
}
