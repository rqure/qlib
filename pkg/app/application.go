package app

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Application interface {
	AddWorker(Worker)
	Execute()
}

type Handle interface {
	DoInMainThread(func())
	GetCtx() context.Context
	GetWg() *sync.WaitGroup
	Quit()
}

type Worker interface {
	Deinit()
	Init(Handle)
	DoWork()
}

type ApplicationImpl struct {
	workers []Worker
	tasks   chan func()
	ctx     context.Context
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
	ticker  *time.Ticker
}

func NewApplication(name string) Application {
	ctx, cancel := context.WithCancel(context.Background())
	a := &ApplicationImpl{
		tasks:  make(chan func(), 1000),
		ctx:    ctx,
		cancel: cancel,
		wg:     &sync.WaitGroup{},
		ticker: time.NewTicker(100 * time.Millisecond),
	}

	SetApplicationName(name)
	return a
}

func (a *ApplicationImpl) AddWorker(w Worker) {
	a.workers = append(a.workers, w)
}

func (a *ApplicationImpl) Init() {
	for _, w := range a.workers {
		w.Init(a)
	}
}

func (a *ApplicationImpl) Deinit() {
	a.ticker.Stop()

	for _, w := range a.workers {
		w.Deinit()
	}

	a.wg.Wait()
}

func (a *ApplicationImpl) Execute() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	a.Init()
	defer a.Deinit()

	defer signal.Stop(interrupt)
	defer a.cancel() // Ensure context is cancelled when Execute returns

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-interrupt:
			return
		case <-a.ticker.C:
			for _, w := range a.workers {
				w.DoWork()
			}
		case task := <-a.tasks:
			task()
		}
	}
}

func (a *ApplicationImpl) GetCtx() context.Context {
	return a.ctx
}

func (a *ApplicationImpl) DoInMainThread(t func()) {
	go func() { a.tasks <- t }()
}

func (a *ApplicationImpl) GetWg() *sync.WaitGroup {
	return a.wg
}

func (a *ApplicationImpl) Quit() {
	a.cancel()
}
