package qapp

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qlog"
)

type Application interface {
	AddWorker(Worker)
	Execute()
}

type Worker interface {
	Deinit(context.Context)
	Init(context.Context)
	DoWork(context.Context)
}

type application struct {
	workers []Worker
	tasks   chan func(context.Context)
	wg      *sync.WaitGroup
	ticker  *time.Ticker

	ctxKVs map[any]any
}

type ApplicationOpts func(map[any]any)

func NewApplication(name string, opts ...ApplicationOpts) Application {
	a := &application{
		tasks:  make(chan func(context.Context), 1000),
		wg:     &sync.WaitGroup{},
		ctxKVs: make(map[any]any),
	}

	a.ctxKVs[qcontext.KeyAppName] = name
	a.ctxKVs[qcontext.KeyAppTickRate] = 100 * time.Millisecond
	a.ctxKVs[qcontext.KeyAppHandle] = a
	a.ctxKVs[qcontext.KeyClientProvider] = qauth.NewClientProvider()

	a.ApplyOpts(opts...)

	a.ticker = time.NewTicker(a.ctxKVs[qcontext.KeyAppTickRate].(time.Duration))

	return a
}

func (a *application) ApplyOpts(opts ...ApplicationOpts) Application {
	for _, opt := range opts {
		opt(a.ctxKVs)
	}

	return a
}

func (a *application) AddWorker(w Worker) {
	a.workers = append(a.workers, w)
}

func (a *application) Init() {
	qlog.Info("Initializing workers")

	ctx, cancel := context.WithTimeout(makeContextWithKV(context.Background(), a.ctxKVs), 10*time.Second)
	defer cancel()

	for _, w := range a.workers {
		w.Init(ctx)
	}

	if ctx.Err() != nil {
		qlog.Panic("Failed to initialize workers")
	}
}

func (a *application) Deinit() {
	qlog.Info("Deinitializing workers")

	ctx, cancel := context.WithTimeout(makeContextWithKV(context.Background(), a.ctxKVs), 10*time.Second)
	defer cancel()

	a.ticker.Stop()

	for _, w := range a.workers {
		w.Deinit(ctx)
	}

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		qlog.Panic("Failed to wait for workers to deinitalize")
	case <-done:
		qlog.Info("All workers have deinitialized")
	}
}

func makeContextWithKV(ctx context.Context, kv map[any]any) context.Context {
	for k, v := range kv {
		ctx = context.WithValue(ctx, k, v)
	}

	return ctx
}

func (a *application) Execute() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)

	ctx, cancel := context.WithCancel(makeContextWithKV(context.Background(), a.ctxKVs))
	defer cancel()

	go func() {
		select {
		case <-interrupt:
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	a.Init()
	defer a.Deinit()

	qlog.Info("Application execution started")
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.ticker.C:
			for _, w := range a.workers {
				w.DoWork(ctx)
			}
		case task := <-a.tasks:
			task(ctx)
		}
	}
}

func (a *application) DoInMainThread(t func(context.Context)) {
	go func() { a.tasks <- t }()
}

func (a *application) GetWg() *sync.WaitGroup {
	return a.wg
}
