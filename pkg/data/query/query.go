package query

import (
	"cmp"
	"context"
	"time"

	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
)

type Query struct {
	store      data.Store
	entityType string
	conditions []Condition
}

func New(s data.Store) data.Query {
	return &Query{
		store: s,
	}
}

func (q *Query) ForType(t string) data.Query {
	q.entityType = t
	return q
}

func (q *Query) Where(fieldName string) data.FieldQuery {
	return &Field{
		query:     q,
		fieldName: fieldName,
	}
}

func (q *Query) Execute(ctx context.Context) []data.EntityBinding {
	if q.entityType == "" {
		return nil
	}

	var results []data.EntityBinding
	fieldsByEntityId := make(map[string]map[string]data.FieldBinding)

	// Do a bulk read from the store
	multi := binding.NewMulti(q.store)
	entities := q.store.FindEntities(ctx, q.entityType)

	// Initialize maps for each entity
	for _, entityId := range entities {
		fieldsByEntityId[entityId] = make(map[string]data.FieldBinding)
	}

	// Bulk read all required fields
	for _, entityId := range entities {
		for _, condition := range q.conditions {
			f := binding.NewField(multi, entityId, condition.fieldName)
			f.ReadValue(ctx)
			fieldsByEntityId[entityId][condition.fieldName] = f
		}
	}
	multi.Commit(ctx)

	// Evaluate conditions using the cached values
	for _, entityId := range entities {
		if q.evaluateConditions(fieldsByEntityId[entityId]) {
			results = append(results, binding.NewEntity(ctx, q.store, entityId))
		}
	}

	return results
}

func (q *Query) evaluateConditions(fieldsByName map[string]data.FieldBinding) bool {
	for _, condition := range q.conditions {
		field := fieldsByName[condition.fieldName]
		switch condition.op {
		case "eq":
			if !q.compareValues(field.GetValue(), condition.value, 0) {
				return false
			}
		case "neq":
			if q.compareValues(field.GetValue(), condition.value, 0) {
				return false
			}
		case "gt":
			if !q.compareValues(field.GetValue(), condition.value, 1) {
				return false
			}
		case "lt":
			if !q.compareValues(field.GetValue(), condition.value, -1) {
				return false
			}
		case "gte":
			if q.compareValues(field.GetValue(), condition.value, -1) {
				return false
			}
		case "lte":
			if q.compareValues(field.GetValue(), condition.value, 1) {
				return false
			}
		}
	}
	return true
}

func (q *Query) compareValues(a data.Value, b any, want int) bool {
	if a == nil || b == nil {
		return false
	}

	switch v := b.(type) {
	case int:
		if a.IsInt() {
			return cmp.Compare(a.GetInt(), int64(v)) == want
		} else if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		}
	case int32:
		if a.IsInt() {
			return cmp.Compare(a.GetInt(), int64(v)) == want
		} else if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		}
	case int64:
		if a.IsInt() {
			return cmp.Compare(a.GetInt(), v) == want
		} else if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		}
	case uint:
		if a.IsInt() {
			return cmp.Compare(a.GetInt(), int64(v)) == want
		} else if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		}
	case uint32:
		if a.IsInt() {
			return cmp.Compare(a.GetInt(), int64(v)) == want
		} else if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		}
	case uint64:
		if a.IsInt() {
			return cmp.Compare(a.GetInt(), int64(v)) == want
		} else if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		}
	case float32:
		if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), float64(v)) == want
		} else if a.IsInt() {
			return cmp.Compare(float64(a.GetInt()), float64(v)) == want
		}
	case float64:
		if a.IsFloat() {
			return cmp.Compare(a.GetFloat(), v) == want
		} else if a.IsInt() {
			return cmp.Compare(float64(a.GetInt()), v) == want
		}
	case string:
		if a.IsString() {
			return cmp.Compare(a.GetString(), v) == want
		} else if a.IsEntityReference() {
			return cmp.Compare(a.GetEntityReference(), v) == want
		} else if a.IsBinaryFile() {
			return cmp.Compare(a.GetBinaryFile(), v) == want
		}
	case bool:
		if a.IsBool() {
			if want == 0 {
				return a.GetBool() == v
			}

			// For non-equality comparisons of bools, true > false
			if want == 1 {
				return a.GetBool() && !v
			} else if want == -1 {
				return !a.GetBool() && v
			}

			return false
		}
	case time.Time:
		if a.IsTimestamp() {
			return cmp.Compare(a.GetTimestamp().Unix(), v.Unix()) == want
		}
	default:
		return false
	}

	return false
}
