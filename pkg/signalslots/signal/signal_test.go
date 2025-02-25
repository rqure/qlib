package signal

import (
	"context"
	"testing"

	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/slot"
)

type TestReceiver struct {
	lastValue interface{}
	callCount int
}

func (r *TestReceiver) Method(val interface{}) {
	r.lastValue = val
	r.callCount++
}

func (r *TestReceiver) VariadicMethod(prefix string, args ...interface{}) {
	result := make([]interface{}, 0, len(args)+1)
	result = append(result, prefix)

	// Flatten any slices in the args
	for _, arg := range args {
		if slice, ok := arg.([]interface{}); ok {
			result = append(result, slice...)
		} else {
			result = append(result, arg)
		}
	}

	r.lastValue = result
	r.callCount++
}

func (r *TestReceiver) ContextMethod(ctx context.Context, val interface{}) {
	r.lastValue = val
	r.callCount++
}

func TestSignal(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (signalslots.Signal, *TestReceiver)
		emit     []interface{}
		validate func(t *testing.T, r *TestReceiver)
	}{
		{
			name: "regular function",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(func(val interface{}) {
					r.lastValue = val
					r.callCount++
				})
				return s, r
			},
			emit: []interface{}{"test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call, got %d", r.callCount)
				}
				if r.lastValue != "test" {
					t.Errorf("Expected value 'test', got %v", r.lastValue)
				}
			},
		},
		{
			name: "method connection",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(r.Method)
				return s, r
			},
			emit: []interface{}{42},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call, got %d", r.callCount)
				}
				if r.lastValue != 42 {
					t.Errorf("Expected value 42, got %v", r.lastValue)
				}
			},
		},
		{
			name: "variadic method",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(r.VariadicMethod)
				return s, r
			},
			emit: []interface{}{"prefix", 1, 2, 3},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call, got %d", r.callCount)
				}
				vals, ok := r.lastValue.([]interface{})
				if !ok {
					t.Fatalf("Expected []interface{}, got %T", r.lastValue)
				}
				if len(vals) != 4 || vals[0] != "prefix" || vals[1] != 1 || vals[2] != 2 || vals[3] != 3 {
					t.Errorf("Expected [prefix 1 2 3], got %v", vals)
				}
			},
		},
		{
			name: "context method",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(r.ContextMethod)
				return s, r
			},
			emit: []interface{}{context.Background(), "test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call, got %d", r.callCount)
				}
				if r.lastValue != "test" {
					t.Errorf("Expected value 'test', got %v", r.lastValue)
				}
			},
		},
		{
			name: "type conversion",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(func(val int) {
					r.lastValue = val
					r.callCount++
				})
				return s, r
			},
			emit: []interface{}{int64(42)}, // Should convert int64 to int
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call, got %d", r.callCount)
				}
				if r.lastValue != 42 {
					t.Errorf("Expected value 42, got %v", r.lastValue)
				}
			},
		},
		{
			name: "multiple slots",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(func(val interface{}) {
					r.callCount++
				})
				s.Connect(func(val interface{}) {
					r.callCount++
				})
				return s, r
			},
			emit: []interface{}{"test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 2 {
					t.Errorf("Expected 2 calls, got %d", r.callCount)
				}
			},
		},
		{
			name: "disconnect",
			setup: func() (signalslots.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				fn1 := func(args ...interface{}) {
					if len(args) > 0 {
						r.callCount++
					}
				}
				fn2 := func(val interface{}) {
					r.callCount++
				}
				slot1 := slot.New(fn1)
				s.Connect(slot1)
				s.Connect(fn2)
				s.Disconnect(slot1)
				return s, r
			},
			emit: []interface{}{"test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call after disconnect, got %d", r.callCount)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			signal, receiver := tt.setup()
			signal.Emit(tt.emit...)
			tt.validate(t, receiver)
		})
	}
}

func TestDisconnectAll(t *testing.T) {
	s := New()
	r := &TestReceiver{}

	s.Connect(func(val interface{}) {
		r.callCount++
	})
	s.Connect(func(val interface{}) {
		r.callCount++
	})

	s.Emit("test")
	if r.callCount != 2 {
		t.Errorf("Expected 2 calls before DisconnectAll, got %d", r.callCount)
	}

	s.DisconnectAll()
	s.Emit("test")
	if r.callCount != 2 {
		t.Errorf("Expected no additional calls after DisconnectAll, still got %d", r.callCount)
	}
}
