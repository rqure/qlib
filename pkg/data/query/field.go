package query

import "github.com/rqure/qlib/pkg/data"

type Field struct {
	query     *Query
	fieldName string
}

func (f *Field) Equals(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "eq",
		value:     value,
	})
	return f.query
}

func (f *Field) NotEquals(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "neq",
		value:     value,
	})
	return f.query
}

func (f *Field) GreaterThan(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "gt",
		value:     value,
	})
	return f.query
}

func (f *Field) LessThan(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "lt",
		value:     value,
	})
	return f.query
}

func (f *Field) LessThanOrEqual(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "lte",
		value:     value,
	})
	return f.query
}

func (f *Field) GreaterThanOrEqual(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "gte",
		value:     value,
	})
	return f.query
}

func (f *Field) Contains(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "contains",
		value:     value,
	})
	return f.query
}

func (f *Field) NotContains(value any) data.Query {
	f.query.conditions = append(f.query.conditions, Condition{
		fieldName: f.fieldName,
		op:        "notcontains",
		value:     value,
	})
	return f.query
}
