package qcontext

import (
	"context"
	"sync"
	"time"

	"github.com/rqure/qlib/pkg/qauthorization"
	"github.com/rqure/qlib/pkg/qss"
)

type CtxType string

const KeyAppName CtxType = "app_name"
const KeyAppTickRate CtxType = "app_tick_rate"
const KeyAppHandle CtxType = "app_handle"
const KeyClientProvider CtxType = "client_provider"
const KeyAuthorizer CtxType = "authorizer"

type ClientProvider[T any] interface {
	Client(ctx context.Context) T
}

type Handle interface {
	Ctx() context.Context
	BusyEvent() qss.Signal[time.Duration]
	DoInMainThread(func(context.Context))
	Exit()
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

func GetAuthorizer(ctx context.Context) (qauthorization.Authorizer, bool) {
	a, ok := ctx.Value(KeyAuthorizer).(qauthorization.Authorizer)
	return a, ok
}
