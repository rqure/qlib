package data

import "time"

type WriteOpt int

const (
	WriteNormal WriteOpt = iota
	WriteChanges
)

type Request interface {
	GetEntityId() string
	GetFieldName() string
	GetWriteTime() *time.Time
	GetWriter() *string
	GetValue() Value
	IsSuccessful() bool

	SetEntityId(string) Request
	SetFieldName(string) Request
	SetWriteTime(*time.Time) Request
	SetWriter(*string) Request
	SetValue(Value) Request
	SetSuccessful(bool) Request

	GetWriteOpt() WriteOpt
	SetWriteOpt(WriteOpt) Request
}
