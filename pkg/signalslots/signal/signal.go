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
	if slot, ok := i.(signalslots.Slot); ok {
		s.slots = append(s.slots, slot)
		return
	}

	dynSlot, err := slot.NewDynamicSlot(i)
	if err != nil {
		log.Error("Cannot create dynamic slot: %v", err)
		return
	}

	s.slots = append(s.slots, dynSlot)
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
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log but don't re-panic so other slots still execute
					log.Error("Slot panicked: %v", r)
				}
			}()
			st.Invoke(args...)
		}()
	}
}

func (s *SignalImpl) DisconnectAll() {
	s.slots = []signalslots.Slot{}
}
