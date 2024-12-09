package signalslots

import "sync"

type Slot[T any] chan T

type Signal[T any] interface {
	Connect(Slot[T])
	Disconnect(chan T)
	DisconnectAll()
	Emit(T)
}

func NewSignal[T any]() Signal[T] {
	return &SignalImpl[T]{}
}

func NewSlot[T any]() Slot[T] {
	return make(chan T)
}

// SignalImpl is a struct that manages connected slots
type SignalImpl[T any] struct {
	slots []Slot[T]
	mu    sync.Mutex
}

// Connect adds a new slot (channel) to the signal
func (s *SignalImpl[T]) Connect(slot Slot[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slots = append(s.slots, slot)
}

// Disconnect removes a slot (channel) from the signal
func (s *SignalImpl[T]) Disconnect(slot chan T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, connectedSlot := range s.slots {
		if connectedSlot == slot {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
	close(slot) // Close the slot channel
}

func (s *SignalImpl[T]) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range s.slots {
		close(slot)
	}
	s.slots = make([]Slot[T], 0)
}

// Emit sends data to all connected slots
func (s *SignalImpl[T]) Emit(data T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range s.slots {
		go func(s Slot[T]) {
			s <- data
		}(slot)
	}
}
