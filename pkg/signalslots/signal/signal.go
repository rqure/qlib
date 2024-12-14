package signal

import (
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/slot"
)

type SignalImpl struct {
	slots []signalslots.Slot
}

func New() signalslots.Signal {
	return &SignalImpl{}
}

func (s *SignalImpl) Connect(i interface{}) {
	switch st := i.(type) {
	case signalslots.Slot:
		s.slots = append(s.slots, st)
	case func(...interface{}):
		s.slots = append(s.slots, slot.SlotWithArgs(st))
	case func():
		s.slots = append(s.slots, slot.SlotWithoutArgs(st))
	default:
		log.Error("Unknown slot type: %v", i)
	}
}

func (s *SignalImpl) Disconnect(st signalslots.Slot) {
	for i, v := range s.slots {
		if v == st {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
}

func (s *SignalImpl) Emit(args ...interface{}) {
	for _, st := range s.slots {
		st.Invoke(args...)
	}
}

func (s *SignalImpl) DisconnectAll() {
	s.slots = []signalslots.Slot{}
}
