package data

import "time"

type Request interface {
	GetEntityId() string
	GetFieldName() string
	GetWriteTime() time.Time
	GetWriter() string
	GetValue() Value
	IsSuccessful() bool

	SetEntityId(string) Request
	SetFieldName(string) Request
	SetWriteTime(time.Time) Request
	SetWriter(string) Request
	SetValue(Value) Request
	SetSuccessful(bool) Request
}
