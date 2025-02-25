package slot

import (
	"testing"
)

func TestDynamicSlot(t *testing.T) {
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
