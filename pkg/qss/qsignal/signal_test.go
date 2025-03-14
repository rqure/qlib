package qsignal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rqure/qlib/pkg/qss"
	"github.com/rqure/qlib/pkg/qss/qslot"
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
		setup    func() (qss.Signal, *TestReceiver)
		emit     []interface{}
		validate func(t *testing.T, r *TestReceiver)
	}{
		{
			name: "regular function",
			setup: func() (qss.Signal, *TestReceiver) {
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
			setup: func() (qss.Signal, *TestReceiver) {
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
			setup: func() (qss.Signal, *TestReceiver) {
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
			setup: func() (qss.Signal, *TestReceiver) {
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
			setup: func() (qss.Signal, *TestReceiver) {
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
			setup: func() (qss.Signal, *TestReceiver) {
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
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				var slot1 qss.Slot = qslot.New(func(args ...interface{}) {
					if len(args) > 0 {
						r.callCount++
					}
				})
				s.Connect(slot1)
				s.Connect(func(val interface{}) {
					r.callCount++
				})
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

type CustomType struct {
	value int
}

func (c *CustomType) Signal(x int) {
	c.value = x
}

func TestSignalAdvanced(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (qss.Signal, *TestReceiver)
		emit     []interface{}
		validate func(t *testing.T, r *TestReceiver)
	}{
		{
			name: "reconnect same slot",
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				fn := func(val interface{}) {
					r.callCount++
				}
				s.Connect(fn)
				s.Connect(fn) // Connect same function twice
				return s, r
			},
			emit: []interface{}{"test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 2 {
					t.Errorf("Expected 2 calls for duplicate connection, got %d", r.callCount)
				}
			},
		},
		{
			name: "emit nil value",
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(func(val interface{}) {
					r.lastValue = val
				})
				return s, r
			},
			emit: []interface{}{nil},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.lastValue != nil {
					t.Errorf("Expected nil value, got %v", r.lastValue)
				}
			},
		},
		{
			name: "emit multiple values",
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(func(a, b, c interface{}) {
					r.lastValue = []interface{}{a, b, c}
				})
				return s, r
			},
			emit: []interface{}{1, "two", true},
			validate: func(t *testing.T, r *TestReceiver) {
				vals, ok := r.lastValue.([]interface{})
				if !ok || len(vals) != 3 {
					t.Fatalf("Expected []interface{} with 3 values, got %T with %v", r.lastValue, r.lastValue)
				}
				if vals[0] != 1 || vals[1] != "two" || vals[2] != true {
					t.Errorf("Expected [1 two true], got %v", vals)
				}
			},
		},
		{
			name: "custom type method",
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				c := &CustomType{}
				s.Connect(c.Signal)
				s.Connect(func(x int) {
					r.lastValue = x
				})
				return s, r
			},
			emit: []interface{}{42},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.lastValue != 42 {
					t.Errorf("Expected value 42, got %v", r.lastValue)
				}
			},
		},
		{
			name: "disconnect non-existent slot",
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				s.Connect(func(val interface{}) {
					r.callCount++
				})
				s.Disconnect(qslot.New(func(...interface{}) {}))
				return s, r
			},
			emit: []interface{}{"test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call, got %d", r.callCount)
				}
			},
		},
		{
			name: "connect after disconnect",
			setup: func() (qss.Signal, *TestReceiver) {
				s := New()
				r := &TestReceiver{}
				fn := func(args ...interface{}) {
					if len(args) > 0 {
						r.callCount++
					}
				}
				sl := qslot.New(fn)
				s.Connect(sl)
				s.Disconnect(sl)
				s.Connect(fn)
				return s, r
			},
			emit: []interface{}{"test"},
			validate: func(t *testing.T, r *TestReceiver) {
				if r.callCount != 1 {
					t.Errorf("Expected 1 call after reconnect, got %d", r.callCount)
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

func TestEmitStress(t *testing.T) {
	s := New()
	r := &TestReceiver{}
	count := 1000

	// Connect multiple slots
	for i := 0; i < 10; i++ {
		s.Connect(func(val interface{}) {
			r.callCount++
		})
	}

	// Emit many times
	for i := 0; i < count; i++ {
		s.Emit(i)
	}

	expected := count * 10
	if r.callCount != expected {
		t.Errorf("Expected %d calls, got %d", expected, r.callCount)
	}
}

func TestConcurrentEmit(t *testing.T) {
	s := New()
	var counter int32
	const numGoroutines = 10
	const emitsPerGoroutine = 100

	s.Connect(func(val interface{}) {
		atomic.AddInt32(&counter, 1)
	})

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < emitsPerGoroutine; j++ {
				s.Emit("test")
			}
		}()
	}

	wg.Wait()

	expected := int32(numGoroutines * emitsPerGoroutine)
	if atomic.LoadInt32(&counter) != expected {
		t.Errorf("Expected %d calls in concurrent test, got %d", expected, atomic.LoadInt32(&counter))
	}
}

func TestSlotPanic(t *testing.T) {
	s := New()
	r := &TestReceiver{}
	panicMsg := "intentional panic"

	// First slot panics
	s.Connect(func(val interface{}) {
		panic(panicMsg)
	})

	// Second slot should still execute
	s.Connect(func(val interface{}) {
		r.callCount++
	})

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Signal should handle slot panics, got: %v", r)
		}
	}()

	s.Emit("test")

	if r.callCount != 1 {
		t.Errorf("Expected second slot to be called once, got %d", r.callCount)
	}
}

func TestComplexMethodChain(t *testing.T) {
	s := New()
	var mu sync.Mutex
	called := make([]int, 0, 5)

	// Connect 5 slots
	for i := 0; i < 5; i++ {
		i := i // Capture loop variable
		s.Connect(func(val interface{}) {
			mu.Lock()
			called = append(called, i)
			mu.Unlock()
		})
	}

	// Emit once to trigger the chain
	s.Emit("start")

	// Verify each slot was called exactly once
	seen := make(map[int]bool)
	for _, id := range called {
		if seen[id] {
			t.Errorf("Slot %d was called multiple times", id)
		}
		seen[id] = true
	}

	if len(called) != 5 {
		t.Errorf("Expected 5 slot calls, got %d", len(called))
	}

	// Verify all slots were called
	for i := 0; i < 5; i++ {
		if !seen[i] {
			t.Errorf("Slot %d was never called", i)
		}
	}
}

func TestSignalChain(t *testing.T) {
	s := New()
	counts := make([]int32, 5)

	// Connect 5 slots that atomically increment their counter
	for i := 0; i < 5; i++ {
		i := i // Capture loop variable
		s.Connect(func(val interface{}) {
			atomic.AddInt32(&counts[i], 1)
		})
	}

	// Emit once
	s.Emit("start")

	// Verify each slot was called exactly once
	for i, count := range counts {
		if count != 1 {
			t.Errorf("Slot %d: got %d calls, want 1", i, count)
		}
	}
}
