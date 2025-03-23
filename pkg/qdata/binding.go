package qdata

type EntityBinding interface {
	Entity
	GetField(string) FieldBinding
}
