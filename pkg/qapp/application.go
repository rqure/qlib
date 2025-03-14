package qapp

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rqure/qlib/pkg/qlog"
)

type Application interface {
	AddWorker(Worker)
	Execute()
}

type HandleKeyType string

const HandleKey HandleKeyType = "handle"

type Handle interface {
	DoInMainThread(func(context.Context))
	GetWg() *sync.WaitGroup
}

type Worker interface {
	Deinit(context.Context)
	Init(context.Context)
	DoWork(context.Context)
}

type ApplicationImpl struct {
	workers []Worker
	tasks   chan func(context.Context)
	wg      *sync.WaitGroup
	ticker  *time.Ticker
}

func NewApplication(name string) Application {
	a := &ApplicationImpl{
		tasks:  make(chan func(context.Context), 1000),
		wg:     &sync.WaitGroup{},
		ticker: time.NewTicker(GetTickRate()),
	}

	SetName(name)

	return a
}

func (a *ApplicationImpl) AddWorker(w Worker) {
	a.workers = append(a.workers, w)
}

func (a *ApplicationImpl) Init() {
	qlog.Info("Initializing workers")

	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), HandleKey, a), 10*time.Second)
	defer cancel()

	for _, w := range a.workers {
		w.Init(ctx)
	}

	if ctx.Err() != nil {
		qlog.Panic("Failed to initialize workers")
	}
}

func (a *ApplicationImpl) Deinit() {
	qlog.Info("Deinitializing workers")

	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), HandleKey, a), 10*time.Second)
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

func (a *ApplicationImpl) Execute() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), HandleKey, a))
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

func (a *ApplicationImpl) DoInMainThread(t func(context.Context)) {
	go func() { a.tasks <- t }()
}

func (a *ApplicationImpl) GetWg() *sync.WaitGroup {
	return a.wg
}

func GetHandle(ctx context.Context) Handle {
	return ctx.Value(HandleKey).(Handle)
}
