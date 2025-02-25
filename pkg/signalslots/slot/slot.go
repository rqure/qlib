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
	v := reflect.ValueOf(i)
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
		if paramType.Kind() == reflect.Interface {
			// For interface parameters, check if type implements interface
			if !val.Type().Implements(paramType) {
				panic(fmt.Sprintf("type %v does not implement %v", val.Type(), paramType))
			}
			callArgs[i] = val
		} else if paramType.Kind() == reflect.String {
			// Special case for strings - only allow string types
			if val.Type().Kind() != reflect.String {
				panic(fmt.Sprintf("cannot convert type %v to string", val.Type()))
			}
			callArgs[i] = val
		} else if val.Type().ConvertibleTo(paramType) {
			// Allow type conversion for other compatible types
			callArgs[i] = val.Convert(paramType)
		} else {
			// For incompatible types, panic
			panic(fmt.Sprintf("cannot convert type %v to %v", val.Type(), paramType))
		}
	}

	s.fn.Call(callArgs)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
