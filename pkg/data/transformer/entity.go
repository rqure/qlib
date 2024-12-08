package transformer

import (
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

func (te *TengoEntity) ToTengoMap() tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"getId": &tengo.UserFunction{
				Name:  "getId",
				Value: te.GetId,
			},
			"getType": &tengo.UserFunction{
				Name:  "getType",
				Value: te.GetType,
			},
			"getChildrenIds": &tengo.UserFunction{
				Name:  "getChildrenIds",
				Value: te.GetChildrenIds,
			},
			"getParentId": &tengo.UserFunction{
				Name:  "getParenetId",
				Value: te.GetParentId,
			},
			"getField": &tengo.UserFunction{
				Name:  "getField",
				Value: te.GetField,
			},
		},
	}
}

func (te *TengoEntity) GetId(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: te.e.GetId()}, nil
}

func (te *TengoEntity) GetType(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: te.e.GetType()}, nil
}

func (te *TengoEntity) GetChildrenIds(...tengo.Object) (tengo.Object, error) {
	children := make([]tengo.Object, 0)
	for _, c := range te.e.GetChildrenIds() {
		children = append(children, &tengo.String{Value: c})
	}

	return &tengo.Array{Value: children}, nil
}

func (te *TengoEntity) GetParentId(...tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: te.e.GetParentId()}, nil
}

func (te *TengoEntity) GetField(args ...tengo.Object) (tengo.Object, error) {
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

	f := binding.New(te.s, te.e.GetId(), fn)

	return NewTengoField(f).ToTengoMap(), nil
}
