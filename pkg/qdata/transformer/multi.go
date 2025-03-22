package transformer

import (
	"context"

	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/qdata"
)

type TengoMulti struct {
	m qdata.BulkFieldOperator
}

func NewTengoMulti(m qdata.BulkFieldOperator) *TengoMulti {
	return &TengoMulti{m: m}
}

func (tm *TengoMulti) ToTengoMap(ctx context.Context) tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"getEntity": &tengo.UserFunction{
				Name: "getEntity",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tm.GetEntity(ctx, args...)
				},
			},
			"commit": &tengo.UserFunction{
				Name: "commit",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return tm.Commit(ctx, args...)
				},
			},
		},
	}
}

func (tm *TengoMulti) GetEntity(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	entityId, ok := tengo.ToString(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "entityId",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	e := tm.m.GetEntityById(ctx, entityId)
	return NewTengoEntity(tm.m, e).ToTengoMap(ctx), nil
}

func (tm *TengoMulti) Commit(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	tm.m.Commit(ctx)
	return tengo.UndefinedValue, nil
}
