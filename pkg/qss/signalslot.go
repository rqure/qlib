package qss

import "github.com/rqure/qlib/pkg/qlog"

type VoidType struct{}

var Void = VoidType{}

type Slot[T any] interface {
	Invoke(T)
}

type Signal[T any] interface {
	Connect(func(T))
	Disconnect(Slot[T])
	DisconnectAll()
	Emit(T)
}

type SlotFn[T any] func(T)

func (s SlotFn[T]) Invoke(arg T) {
	s(arg)
}

type SignalImpl[T any] struct {
	slots []Slot[T]
}

func New[T any]() Signal[T] {
	return &SignalImpl[T]{}
}

func (s *SignalImpl[T]) Connect(fn func(T)) {
	s.slots = append(s.slots, SlotFn[T](fn))
}

func (s *SignalImpl[T]) Disconnect(st Slot[T]) {
	for i, v := range s.slots {
		if v == st {
			s.slots = append(s.slots[:i], s.slots[i+1:]...)
			break
		}
	}
}

func (s *SignalImpl[T]) Emit(arg T) {
	for _, st := range s.slots {
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Log but don't re-panic so other slots still execute
					qlog.Error("Slot panicked: %v", r)
				}
			}()
			st.Invoke(arg)
		}()
	}
}

func (s *SignalImpl[T]) DisconnectAll() {
	s.slots = []Slot[T]{}
}
