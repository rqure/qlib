package qquery

import (
	"cmp"
	"context"
	"time"

	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qdata/qbinding"
)

type Query struct {
	store      qdata.Store
	entityType string
	fields     []string
	conditions []Condition
}

func New(s qdata.Store) qdata.Query {
	return &Query{
		store: s,
	}
}

func (q *Query) ForType(t string) qdata.Query {
	q.entityType = t
	return q
}

func (q *Query) From(t string) qdata.Query {
	return q.ForType(t)
}

func (q *Query) Select(fields ...string) qdata.Query {
	q.fields = append(q.fields, fields...)
	return q
}

func (q *Query) Where(fieldName string) qdata.FieldQuery {
	return &Field{
		query:     q,
		fieldName: fieldName,
	}
}

func (q *Query) Execute(ctx context.Context) []qdata.EntityBinding {
	if q.entityType == "" {
		return nil
	}

	var results []qdata.EntityBinding
	var allEntities []qdata.EntityBinding

	// Do a bulk read from the store
	multi := qbinding.NewMulti(q.store)
	entities := q.store.FindEntities(ctx, q.entityType)

	// Bulk read all required fields
	for _, entityId := range entities {
		ent := qbinding.NewEntity(ctx, q.store, entityId)

		for _, fieldName := range q.fields {
			f := ent.GetField(fieldName)
			f.ReadValue(ctx)
		}

		for _, condition := range q.conditions {
			f := ent.GetField(condition.fieldName)
			f.ReadValue(ctx)
		}

		allEntities = append(allEntities, ent)
	}
	multi.Commit(ctx)

	// Evaluate conditions using the cached values
	for _, e := range allEntities {
		if q.evaluateConditions(e) {
			results = append(results, e)
		}
	}

	return results
}

func (q *Query) evaluateConditions(e qdata.EntityBinding) bool {
	for _, condition := range q.conditions {
		f := e.GetField(condition.fieldName)
		switch condition.op {
		case "eq":
			if !q.compareValues(f.GetValue(), condition.value, 0) {
				return false
			}
		case "neq":
			if q.compareValues(f.GetValue(), condition.value, 0) {
				return false
			}
		case "gt":
			if !q.compareValues(f.GetValue(), condition.value, 1) {
				return false
			}
		case "lt":
			if !q.compareValues(f.GetValue(), condition.value, -1) {
				return false
			}
		case "gte":
			if q.compareValues(f.GetValue(), condition.value, -1) {
				return false
			}
		case "lte":
			if q.compareValues(f.GetValue(), condition.value, 1) {
				return false
			}
		}
	}
	return true
}

func (q *Query) compareValues(a qdata.Value, b any, want int) bool {
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
