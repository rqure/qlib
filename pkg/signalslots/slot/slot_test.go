package slot

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type customType int
type stringAlias string

func (c customType) String() string {
	return "custom"
}

func TestDynamicSlot(t *testing.T) {
	ch := make(chan int)
	ptr := new(int)
	var ct customType = 42
	var sa stringAlias = "alias"

	tests := []struct {
		name      string
		fn        interface{}
		args      []interface{}
		wantPanic bool
	}{
		{
			name: "nil argument",
			fn: func(val *string) {
				// Should handle nil
			},
			args: []interface{}{nil},
		},
		{
			name: "wrong number of arguments",
			fn: func(a, b string) {
			},
			args:      []interface{}{"one"},
			wantPanic: true,
		},
		{
			name: "incompatible type",
			fn: func(val string) {
			},
			args:      []interface{}{42},
			wantPanic: true,
		},
		{
			name: "interface type",
			fn: func(val interface{}) {
			},
			args: []interface{}{42},
		},
		{
			name: "variadic with no args",
			fn: func(prefix string, args ...interface{}) {
			},
			args: []interface{}{"prefix"},
		},
		{
			name: "variadic type conversion",
			fn: func(prefix string, args ...int) {
			},
			args: []interface{}{"prefix", int64(1), int32(2), 3},
		},
		{
			name: "pointer argument",
			fn: func(p *int) {
				*p = 42
			},
			args: []interface{}{ptr},
		},
		{
			name: "channel argument",
			fn: func(c chan int) {
			},
			args: []interface{}{ch},
		},
		{
			name: "custom type implementing interface",
			fn: func(s fmt.Stringer) string {
				return s.String()
			},
			args: []interface{}{ct},
		},
		{
			name: "type alias conversion",
			fn: func(s string) {
			},
			args: []interface{}{sa},
		},
		{
			name: "variadic mixed types",
			fn: func(prefix string, args ...interface{}) {
			},
			args: []interface{}{"prefix", 1, "two", 3.0, true},
		},
		{
			name: "variadic type conversion all args",
			fn: func(args ...int) {
			},
			args: []interface{}{int8(1), int16(2), int32(3), int64(4)},
		},
		{
			name: "empty variadic",
			fn: func(prefix string, args ...int) {
			},
			args: []interface{}{"prefix"},
		},
		{
			name: "function returning values",
			fn: func(x int) int {
				return x * 2
			},
			args: []interface{}{21},
		},
		{
			name:      "nil function",
			fn:        nil,
			args:      []interface{}{},
			wantPanic: true,
		},
		{
			name:      "non-function type",
			fn:        "not a function",
			args:      []interface{}{},
			wantPanic: true,
		},
		{
			name:      "incompatible type",
			fn:        func(s string) {},
			args:      []interface{}{42},
			wantPanic: true,
		},
		{
			name: "interface type",
			fn: func(i interface{}) {
			},
			args: []interface{}{42},
		},
		{
			name: "variadic type conversion",
			fn: func(prefix string, nums ...int) {
			},
			args: []interface{}{"test", int64(1), int32(2), int16(3)},
		},
		{
			name: "type alias",
			fn:   func(s stringAlias) {},
			args: []interface{}{stringAlias("test")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, err := NewDynamicSlot(tt.fn)
			if err != nil {
				if tt.wantPanic {
					return // Expected error
				}
				t.Fatalf("NewDynamicSlot() error = %v", err)
			}

			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("DynamicSlot.Invoke() panic = %v, wantPanic = %v", r, tt.wantPanic)
				}
			}()

			slot.Invoke(tt.args...)
		})
	}
}

type methodReceiver struct {
	called bool
	args   []interface{}
}

func (r *methodReceiver) Method(args ...interface{}) {
	r.called = true
	r.args = args
}

func TestMethodReceiver(t *testing.T) {
	r := &methodReceiver{}

	tests := []struct {
		name string
		fn   interface{}
		args []interface{}
		want []interface{}
	}{
		{
			name: "bound method",
			fn:   r.Method,
			args: []interface{}{1, "two", 3.0},
			want: []interface{}{1, "two", 3.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r.called = false
			r.args = nil

			slot, err := NewDynamicSlot(tt.fn)
			if err != nil {
				t.Fatalf("NewDynamicSlot() error = %v", err)
			}

			slot.Invoke(tt.args...)

			if !r.called {
				t.Error("Method was not called")
			}

			if len(r.args) != len(tt.want) {
				t.Errorf("Got %d args, want %d", len(r.args), len(tt.want))
				return
			}

			for i, want := range tt.want {
				if r.args[i] != want {
					t.Errorf("arg[%d] = %v, want %v", i, r.args[i], want)
				}
			}
		})
	}
}

// Test method receiver on non-struct type
type customTypeWithMethod int

func (c *customTypeWithMethod) Method(x int) int {
	*c += customTypeWithMethod(x)
	return int(*c)
}

func TestMethodTypes(t *testing.T) {
	tests := []struct {
		name string
		fn   interface{}
		args []interface{}
		want interface{}
	}{
		{
			name: "pointer method receiver",
			fn:   (&methodReceiver{}).Method,
			args: []interface{}{1, 2, 3},
			want: []interface{}{1, 2, 3},
		},
		{
			name: "method on custom type",
			fn: func() interface{} {
				var c customTypeWithMethod = 0
				return c.Method
			}(),
			args: []interface{}{5},
			want: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, err := NewDynamicSlot(tt.fn)
			if err != nil {
				t.Fatalf("NewDynamicSlot() error = %v", err)
			}

			// For methods that modify the receiver or return values
			if tt.want != nil {
				switch fn := slot.fn; {
				case fn.Type().NumOut() > 0:
					// Call method and check return value
					rets := fn.Call([]reflect.Value{reflect.ValueOf(tt.args[0])})
					if len(rets) > 0 {
						result := rets[0].Interface()
						if !reflect.DeepEqual(result, tt.want) {
							t.Errorf("got result = %v, want %v", result, tt.want)
						}
					}
				default:
					// Just call the method normally
					slot.Invoke(tt.args...)
				}
			} else {
				slot.Invoke(tt.args...)
			}
		})
	}
}

// Test invalid function signatures
func TestInvalidFunctionSignatures(t *testing.T) {
	tests := []struct {
		name      string
		fn        interface{}
		wantError bool
	}{
		{
			name:      "nil function",
			fn:        nil,
			wantError: true,
		},
		{
			name:      "string value",
			fn:        "not a function",
			wantError: true,
		},
		{
			name:      "channel value",
			fn:        make(chan int),
			wantError: true,
		},
		{
			name:      "struct value",
			fn:        struct{}{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDynamicSlot(tt.fn)
			if (err != nil) != tt.wantError {
				t.Errorf("NewDynamicSlot() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestErrorHandling(t *testing.T) {
	// Test recovery from different panic types
	tests := []struct {
		name     string
		fn       interface{}
		args     []interface{}
		panicVal interface{}
	}{
		{
			name: "panic with error",
			fn: func() {
				panic(errors.New("test error"))
			},
			panicVal: "test error",
		},
		{
			name: "panic with string",
			fn: func() {
				panic("test panic")
			},
			panicVal: "test panic",
		},
		{
			name: "panic with struct",
			fn: func() {
				panic(struct{ msg string }{"test"})
			},
			panicVal: struct{ msg string }{"test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, err := NewDynamicSlot(tt.fn)
			if err != nil {
				t.Fatalf("NewDynamicSlot() error = %v", err)
			}

			var recovered interface{}
			func() {
				defer func() {
					recovered = recover()
				}()
				slot.Invoke(tt.args...)
			}()

			if recovered == nil {
				t.Error("Expected panic, got none")
			} else if fmt.Sprint(recovered) != fmt.Sprint(tt.panicVal) {
				t.Errorf("Got panic value %v, want %v", recovered, tt.panicVal)
			}
		})
	}
}

func TestTypeCompatibility(t *testing.T) {
	tests := []struct {
		name      string
		fn        interface{}
		args      []interface{}
		wantPanic bool
	}{
		{
			name: "interface constraint",
			fn: func(s fmt.Stringer) string {
				return s.String()
			},
			args: []interface{}{customType(42)},
		},
		{
			name: "type alias",
			fn:   func(s string) {},
			args: []interface{}{stringAlias("test")},
		},
		{
			name: "type assertion in slot",
			fn: func(i interface{}) string {
				if s, ok := i.(fmt.Stringer); ok {
					return s.String()
				}
				return ""
			},
			args: []interface{}{customType(42)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, err := NewDynamicSlot(tt.fn)
			if err != nil {
				t.Fatalf("NewDynamicSlot() error = %v", err)
			}

			defer func() {
				r := recover()
				if (r != nil) != tt.wantPanic {
					t.Errorf("DynamicSlot.Invoke() panic = %v, wantPanic = %v", r, tt.wantPanic)
				}
			}()

			slot.Invoke(tt.args...)
		})
	}
}

func TestTypeConversions(t *testing.T) {
	tests := []struct {
		name      string
		fn        interface{}
		args      []interface{}
		wantPanic bool
	}{
		{
			name:      "compatible numeric conversion",
			fn:        func(x int) {},
			args:      []interface{}{int64(42)},
			wantPanic: false,
		},
		{
			name:      "compatible type alias",
			fn:        func(s string) {},
			args:      []interface{}{stringAlias("test")},
			wantPanic: false,
		},
		{
			name:      "incompatible types",
			fn:        func(s string) {},
			args:      []interface{}{42},
			wantPanic: true,
		},
		{
			name:      "interface compliance",
			fn:        func(s fmt.Stringer) {},
			args:      []interface{}{customType(42)},
			wantPanic: false,
		},
		{
			name:      "non-interface to interface",
			fn:        func(s fmt.Stringer) {},
			args:      []interface{}{123}, // int does not implement Stringer
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slot, err := NewDynamicSlot(tt.fn)
			if err != nil {
				t.Fatalf("NewDynamicSlot() error = %v", err)
			}

			var panicked bool
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
						if !tt.wantPanic {
							t.Errorf("unexpected panic: %v", r)
						}
					}
				}()
				slot.Invoke(tt.args...)
			}()

			if tt.wantPanic && !panicked {
				t.Error("expected panic but got none")
			}
		})
	}
}

func TestInvalidSlotCreation(t *testing.T) {
	tests := []struct {
		name    string
		fn      interface{}
		wantErr string
	}{
		{
			name:    "nil function",
			fn:      nil,
			wantErr: "value must be a function, got nil",
		},
		{
			name:    "non-function type",
			fn:      "not a function",
			wantErr: "value must be a function, got string",
		},
		{
			name:    "nil interface",
			fn:      (func())(nil),
			wantErr: "value must be a function, got nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDynamicSlot(tt.fn)
			if err == nil || err.Error() != tt.wantErr {
				t.Errorf("NewDynamicSlot() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
