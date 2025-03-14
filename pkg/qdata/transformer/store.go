package transformer

import (
	"container/heap"
	"context"
	"errors"
	"time"

	"github.com/d5/tengo/v2"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
)

type TengoStore struct {
	impl  qdata.Store
	queue JobQueue
}

func NewTengoStore(s qdata.Store) *TengoStore {
	ts := &TengoStore{impl: s}

	heap.Init(&ts.queue)

	return ts
}

func (ts *TengoStore) ToTengoMap(ctx context.Context) tengo.Object {
	return &tengo.Map{
		Value: map[string]tengo.Object{
			"getEntity": &tengo.UserFunction{
				Name: "getEntity",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return ts.GetEntity(ctx, args...)
				},
			},
			"query": &tengo.UserFunction{
				Name: "query",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return ts.Query(ctx, args...)
				},
			},
			"schedule": &tengo.UserFunction{
				Name: "schedule",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return ts.Schedule(ctx, args...)
				},
			},
			"getMulti": &tengo.UserFunction{
				Name: "getMulti",
				Value: func(args ...tengo.Object) (tengo.Object, error) {
					return ts.GetMulti(ctx, args...)
				},
			},
		},
	}
}

func (ts *TengoStore) GetEntity(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
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

	e := ts.impl.GetEntity(ctx, entityId)
	if e == nil {
		return nil, errors.New("entity not found")
	}

	return NewTengoEntity(ts.impl, e).ToTengoMap(ctx), nil
}

func (ts *TengoStore) Query(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	entityType, ok := tengo.ToString(args[0])
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "entityType",
			Expected: "string",
			Found:    args[0].TypeName(),
		}
	}

	entityIds := ts.impl.FindEntities(ctx, entityType)
	entities := make([]tengo.Object, 0)
	resultEntities := make([]tengo.Object, 0)
	for _, entityId := range entityIds {
		e := ts.impl.GetEntity(ctx, entityId)
		entities = append(entities, NewTengoEntity(ts.impl, e).ToTengoMap(ctx))
	}

	if len(args) > 1 {
		conditionFn, ok := args[1].(*tengo.UserFunction)
		if !ok {
			return nil, &tengo.ErrInvalidArgumentType{
				Name:     "conditionFn",
				Expected: "function",
				Found:    args[1].TypeName(),
			}
		}

		for _, e := range entities {
			met, err := conditionFn.Call(e)
			if err != nil {
				return nil, err
			}

			if met == tengo.TrueValue {
				resultEntities = append(resultEntities, e)
			}
		}
	} else {
		resultEntities = entities
	}

	return &tengo.Array{Value: resultEntities}, nil
}

func (ts *TengoStore) Schedule(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	if len(args) < 1 {
		return nil, tengo.ErrWrongNumArguments
	}

	fn, ok := args[0].(*tengo.UserFunction)
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "fn",
			Expected: "function",
			Found:    args[0].TypeName(),
		}
	}

	deadline, ok := args[1].(*tengo.Time)
	if !ok {
		return nil, &tengo.ErrInvalidArgumentType{
			Name:     "deadline",
			Expected: "time",
			Found:    args[1].TypeName(),
		}
	}

	heap.Push(&ts.queue, &Job{task: fn, deadline: deadline.Value})

	return tengo.UndefinedValue, nil
}

func (ts *TengoStore) PopAvailableJobs() []*Job {
	now := time.Now()
	availableJobs := make([]*Job, 0)
	for ts.queue.Len() > 0 {
		if ts.queue[0].deadline.After(now) {
			break
		}

		availableJobs = append(availableJobs, heap.Pop(&ts.queue).(*Job))
	}

	return availableJobs
}

// Add this new method
func (ts *TengoStore) GetMulti(ctx context.Context, args ...tengo.Object) (tengo.Object, error) {
	multi := qbinding.NewMulti(ts.impl)
	return NewTengoMulti(multi).ToTengoMap(ctx), nil
}
