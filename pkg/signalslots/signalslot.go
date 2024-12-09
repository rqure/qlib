package signalslots

import "sync"

type Slot[T any] chan T

// Signal is a struct that manages connected slots
type Signal[T any] struct {
	slots []Slot[T]
	mu    sync.Mutex
}

// Connect adds a new slot (channel) to the signal
func (s *Signal[T]) Connect(slot Slot[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slots = append(s.slots, slot)
}

// Disconnect removes a slot (channel) from the signal
func (s *Signal[T]) Disconnect(slot chan T) {
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

func (s *Signal[T]) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range s.slots {
		close(slot)
	}
	s.slots = make([]Slot[T], 0)
}

// Emit sends data to all connected slots
func (s *Signal[T]) Emit(data T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range s.slots {
		go func(s Slot[T]) {
			s <- data
		}(slot)
	}
}
