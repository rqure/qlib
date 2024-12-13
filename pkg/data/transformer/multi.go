package transformer

import (
	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/data"
)

type TengoMulti struct {
	m data.MultiBinding
}

func NewTengoMulti(m data.MultiBinding) *TengoMulti {
	return &TengoMulti{m: m}
}

func (tm *TengoMulti) ToTengoMap() tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"getEntity": &tengo.UserFunction{
				Name:  "getEntity",
				Value: tm.GetEntity,
			},
			"commit": &tengo.UserFunction{
				Name:  "commit",
				Value: tm.Commit,
			},
		},
	}
}

func (tm *TengoMulti) GetEntity(args ...tengo.Object) (tengo.Object, error) {
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

	e := tm.m.GetEntityById(entityId)
	return NewTengoEntity(tm.m, e).ToTengoMap(), nil
}

func (tm *TengoMulti) Commit(...tengo.Object) (tengo.Object, error) {
	tm.m.Commit()
	return tengo.UndefinedValue, nil
}
