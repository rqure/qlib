package qcontext

import (
	"context"
	"sync"
	"time"
)

type CtxType string

const KeyAppName CtxType = "app_name"
const KeyAppTickRate CtxType = "app_tick_rate"
const KeyAppHandle CtxType = "app_handle"
const KeyClientProvider CtxType = "client_provider"

type ClientProvider[T any] interface {
	Client(ctx context.Context) T
}

type Handle interface {
	DoInMainThread(func(context.Context))
	GetWg() *sync.WaitGroup
}

func GetHandle(ctx context.Context) Handle {
	return ctx.Value(KeyAppHandle).(Handle)
}

func GetClientProvider[T any](ctx context.Context) ClientProvider[T] {
	return ctx.Value(KeyClientProvider).(ClientProvider[T])
}

func GetAppName(ctx context.Context) string {
	return ctx.Value(KeyAppName).(string)
}

func GetTickRate(ctx context.Context) time.Duration {
	return ctx.Value(KeyAppTickRate).(time.Duration)
}
