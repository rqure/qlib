package qapp

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rqure/qlib/pkg/qauthentication"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
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
	exitCh  chan struct{}

	busyDuration qss.Signal[time.Duration]

	ctx    context.Context
	cancel context.CancelFunc
}

type ApplicationOpts func(map[any]any)

func NewApplication(name string, opts ...ApplicationOpts) Application {
	a := &application{
		tasks:        make(chan func(context.Context), 10000),
		wg:           &sync.WaitGroup{},
		exitCh:       make(chan struct{}, 1),
		busyDuration: qss.New[time.Duration](),
	}

	ctxKVs := make(map[any]any)
	ctxKVs[qcontext.KeyAppName] = name
	ctxKVs[qcontext.KeyAppTickRate] = 100 * time.Millisecond
	ctxKVs[qcontext.KeyAppHandle] = a
	ctxKVs[qcontext.KeyClientProvider] = qauthentication.NewClientProvider()

	a.applyOpts(ctxKVs, opts...)

	a.ticker = time.NewTicker(ctxKVs[qcontext.KeyAppTickRate].(time.Duration))

	a.ctx, a.cancel = context.WithCancel(makeContextWithKV(context.Background(), ctxKVs))

	return a
}

func (me *application) applyOpts(ctxKVs map[any]any, opts ...ApplicationOpts) Application {
	for _, opt := range opts {
		opt(ctxKVs)
	}

	return me
}

func (me *application) AddWorker(w Worker) {
	me.workers = append(me.workers, w)
}

func (me *application) Init() {
	qlog.Info("Initializing workers")

	for _, w := range me.workers {
		w.Init(me.ctx)
	}

	if me.ctx.Err() != nil {
		qlog.Panic("Failed to initialize workers")
	}
}

func (me *application) Deinit() {
	qlog.Info("Deinitializing workers")

	me.ticker.Stop()

	for _, w := range me.workers {
		w.Deinit(me.ctx)
	}

	done := make(chan struct{})
	go func() {
		me.wg.Wait()
		close(done)
	}()

	select {
	case <-me.ctx.Done():
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

func (me *application) Execute() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)

	ctx, cancel := context.WithCancel(me.ctx)
	defer cancel()

	go func() {
		select {
		case <-me.exitCh:
			cancel()
		case <-interrupt:
			cancel()
		case <-ctx.Done():
			return
		}
	}()

	me.Init()
	defer me.Deinit()

	qlog.Info("Application execution started")
	for {
		select {
		case <-ctx.Done():
			return
		case <-me.ticker.C:
			busyStart := time.Now()
			for _, w := range me.workers {
				w.DoWork(ctx)
			}
			me.busyDuration.Emit(time.Since(busyStart))
		case task := <-me.tasks:
			busyStart := time.Now()
			task(ctx)
			me.busyDuration.Emit(time.Since(busyStart))
		}
	}
}

func (me *application) DoInMainThread(t func(context.Context)) {
	go func() { me.tasks <- t }()
}

func (a *application) GetWg() *sync.WaitGroup {
	return a.wg
}

func (me *application) Exit() {
	qlog.Trace("Application exit requested")
	me.exitCh <- struct{}{}
}

func (me *application) BusyEvent() qss.Signal[time.Duration] {
	return me.busyDuration
}

func (me *application) Ctx() context.Context {
	return me.ctx
}
