package transformer

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
)

type TengoEntity struct {
	s qdata.Store
	e qdata.Entity
}

func NewTengoEntity(s qdata.Store, e qdata.Entity) *TengoEntity {
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

	f := qbinding.NewField(&te.s, te.e.GetId(), fn)

	return NewTengoField(f).ToTengoMap(ctx), nil
}
