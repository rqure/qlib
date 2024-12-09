package app

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Application interface {
	AddWorker(Worker)
	Execute()
}

type Handle interface {
	GetCtx() context.Context
	Do(func())
	GetWg() *sync.WaitGroup
}

type Worker interface {
	Deinit()
	Init(Handle)
}

type ApplicationImpl struct {
	workers []Worker
	tasks   chan func()
	ctx     context.Context
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
}

func NewApplication(name string) Application {
	ctx, cancel := context.WithCancel(context.Background())
	a := &ApplicationImpl{
		tasks:  make(chan func()),
		ctx:    ctx,
		cancel: cancel,
		wg:     &sync.WaitGroup{},
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
		case task := <-a.tasks:
			task()
		}
	}
}

func (a *ApplicationImpl) GetCtx() context.Context {
	return a.ctx
}

func (a *ApplicationImpl) Do(t func()) {
	go func() { a.tasks <- t }()
}

func (a *ApplicationImpl) GetWg() *sync.WaitGroup {
	return a.wg
}
