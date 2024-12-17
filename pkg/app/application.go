package app

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rqure/qlib/pkg/log"
)

type Application interface {
	AddWorker(Worker)
	Execute()
}

type Handle interface {
	DoInMainThread(func(context.Context))
	GetWg() *sync.WaitGroup
}

type Worker interface {
	Deinit(context.Context)
	Init(context.Context, Handle)
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
	log.Info("Initializing workers")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, w := range a.workers {
		w.Init(ctx, a)
	}

	if ctx.Err() != nil {
		log.Panic("Failed to initialize workers")
	}
}

func (a *ApplicationImpl) Deinit() {
	log.Info("Deinitializing workers")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		log.Panic("Failed to wait for workers to deinitalize")
	case <-done:
		log.Info("All workers have deinitialized")
	}
}

func (a *ApplicationImpl) Execute() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)

	ctx, cancel := context.WithCancel(context.Background())
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
