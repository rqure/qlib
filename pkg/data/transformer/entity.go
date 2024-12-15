package transformer

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
)

type TengoEntity struct {
	s data.Store
	e data.Entity
}

func NewTengoEntity(s data.Store, e data.Entity) *TengoEntity {
	return &TengoEntity{s: s, e: e}
}

func (te *TengoEntity) ToTengoMap(ctx context.Context) tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"getId": &tengo.UserFunction{
				Name: "getId",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return te.GetId(ctx, args...)
				},
			},
			"getType": &tengo.UserFunction{
				Name: "getType",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return te.GetType(ctx, args...)
				},
			},
			"getChildrenIds": &tengo.UserFunction{
				Name: "getChildrenIds",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return te.GetChildrenIds(ctx, args...)
				},
			},
			"getParentId": &tengo.UserFunction{
				Name: "getParenetId",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return te.GetParentId(ctx, args...)
				},
			},
			"getField": &tengo.UserFunction{
				Name: "getField",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return te.GetField(ctx, args...)
				},
			},
		},
	}
}

func (te *TengoEntity) GetId(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: te.e.GetId()}, nil
}

func (te *TengoEntity) GetType(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: te.e.GetType()}, nil
}

func (te *TengoEntity) GetChildrenIds(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	children := make([]tengo.Object, 0)
	for _, c := range te.e.GetChildrenIds() {
		children = append(children, &tengo.String{Value: c})
	}

	return &tengo.Array{Value: children}, nil
}

func (te *TengoEntity) GetParentId(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: te.e.GetParentId()}, nil
}

func (te *TengoEntity) GetField(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	fn, ok := tengo.ToString(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "fieldName",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	f := binding.NewField(te.s, te.e.GetId(), fn)

	return NewTengoField(f).ToTengoMap(ctx), nil
}
