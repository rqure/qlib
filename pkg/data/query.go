package data

type Query interface {
	ForType(string) Query
	Where(string) FieldQuery
	Execute() []Entity
}

type FieldQuery interface {
	Equals(value any) Query
	NotEquals(value any) Query
	GreaterThan(value any) Query
	LessThan(value any) Query
	LessThanOrEqual(value any) Query
	GreaterThanOrEqual(value any) Query
	Contains(value any) Query
	NotContains(value any) Query
}
