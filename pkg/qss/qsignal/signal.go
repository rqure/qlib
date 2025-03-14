package qsignal

import (
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
	"github.com/rqure/qlib/pkg/qss/qslot"
)

type SignalImpl struct {
	slots []qss.Slot
}

func New() qss.Signal {
	return &SignalImpl{}
}

func (s *SignalImpl) Connect(i interface{}) {
	if slot, ok := i.(qss.Slot); ok {
		s.slots = append(s.slots, slot)
		return
	}

	dynSlot, err := qslot.NewDynamicSlot(i)
	if err != nil {
		qlog.Error("Cannot create dynamic slot: %v", err)
		return
	}

	s.slots = append(s.slots, dynSlot)
}

func (s *SignalImpl) Disconnect(st qss.Slot) {
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
					qlog.Error("Slot panicked: %v", r)
				}
			}()
			st.Invoke(args...)
		}()
	}
}

func (s *SignalImpl) DisconnectAll() {
	s.slots = []qss.Slot{}
}
