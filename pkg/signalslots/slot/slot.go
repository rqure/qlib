package slot

import (
	"fmt"
	"reflect"

	"github.com/rqure/qlib/pkg/signalslots"
)

type SlotWrapper struct {
	Fn func(...interface{})
}

func New(fn func(...interface{})) signalslots.Slot {
	return &SlotWrapper{Fn: fn}
}

func (s *SlotWrapper) Invoke(args ...interface{}) {
	s.Fn(args...)
}

type DynamicSlot struct {
	fn reflect.Value
}

func NewDynamicSlot(i interface{}) (*DynamicSlot, error) {
	if i == nil {
		return nil, fmt.Errorf("value must be a function, got nil")
	}

	v := reflect.ValueOf(i)
	if !v.IsValid() || v.IsNil() {
		return nil, fmt.Errorf("value must be a function, got nil")
	}
	
	if v.Kind() != reflect.Func {
		return nil, fmt.Errorf("value must be a function, got %T", i)
	}
	
	return &DynamicSlot{fn: v}, nil
}

func (s *DynamicSlot) Invoke(args ...interface{}) {
	t := s.fn.Type()
	numIn := t.NumIn()

	if !t.IsVariadic() && len(args) != numIn {
		panic(fmt.Sprintf("wrong number of arguments: got %d, want %d", len(args), numIn))
	}

	if t.IsVariadic() && len(args) < numIn-1 {
		panic(fmt.Sprintf("not enough arguments for variadic function: got %d, want at least %d", len(args), numIn-1))
	}

	callArgs := make([]reflect.Value, len(args))
	for i, arg := range args {
		paramType := t.In(min(i, numIn-1))
		if t.IsVariadic() && i >= numIn-1 {
			paramType = paramType.Elem()
		}

		if arg == nil {
			callArgs[i] = reflect.Zero(paramType)
			continue
		}

		val := reflect.ValueOf(arg)
		if !val.IsValid() {
			callArgs[i] = reflect.Zero(paramType)
			continue
		}

		// Handle type compatibility
		switch {
		case val.Type() == paramType:
			// Exact type match
			callArgs[i] = val
		case paramType.Kind() == reflect.Interface:
			if !val.Type().Implements(paramType) {
				panic(fmt.Sprintf("type %v does not implement %v", val.Type(), paramType))
			}
			callArgs[i] = val
		case val.Type().ConvertibleTo(paramType) && (
			// Allow conversions between numeric types or same-kind types (like type aliases)
			(isNumericKind(paramType.Kind()) && isNumericKind(val.Type().Kind())) ||
			paramType.Kind() == val.Type().Kind()):
			callArgs[i] = val.Convert(paramType)
		default:
			panic(fmt.Sprintf("incompatible types: cannot convert %v to %v", val.Type(), paramType))
		}
	}

	s.fn.Call(callArgs)
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	default:
		return false
	}
}

// isBasicKind returns true if the kind is a basic type that supports conversion
func isBasicKind(k reflect.Kind) bool {
	switch k {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128, reflect.String:
		return true
	default:
		return false
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
