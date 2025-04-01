package qscripting

import (
	"fmt"
	"reflect"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/token"
)

type Any struct {
	Value interface{}
}

func (a *Any) TypeName() string {
	return "Any"
}

func (a *Any) String() string {
	return fmt.Sprintf("%v", a.Value)
}

func (a *Any) IsFalsy() bool {
	return a.Value == nil
}

func (a *Any) IsTruthy() bool {
	return a.Value != nil
}

func (a *Any) IsUndefined() bool {
	return a.Value == nil
}

func (a *Any) IsNil() bool {
	return a.Value == nil
}

func (a *Any) BinaryOp(op token.Token, rhs tengo.Object) (tengo.Object, error) {
	return nil, tengo.ErrInvalidOperator
}

func (a *Any) Equals(another tengo.Object) bool {
	if another.TypeName() != "Any" {
		return false
	}

	v, ok := another.(*Any)
	if !ok {
		return false
	}

	return reflect.DeepEqual(a.Value, v.Value)
}

func (a *Any) Copy() tengo.Object {
	return &Any{Value: a.Value}
}

func (a *Any) IndexGet(index tengo.Object) (value tengo.Object, err error) {
	return nil, tengo.ErrNotIndexable
}

func (a *Any) IndexSet(index, value tengo.Object) error {
	return tengo.ErrNotIndexAssignable
}

func (a *Any) Iterate() tengo.Iterator {
	return nil
}

func (a *Any) CanIterate() bool {
	return false
}

func (a *Any) Call(args ...tengo.Object) (ret tengo.Object, err error) {
	return nil, fmt.Errorf("cannot call %s", a.TypeName())
}

func (a *Any) CanCall() bool {
	return false
}
