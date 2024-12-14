package app

import (
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
	GetWg() *sync.WaitGroup
}

type Worker interface {
	Deinit()
	Init(Handle)
	DoWork()
}

type ApplicationImpl struct {
	workers []Worker
	tasks   chan func()
	wg      *sync.WaitGroup
	ticker  *time.Ticker
}

func NewApplication(name string) Application {
	a := &ApplicationImpl{
		tasks:  make(chan func(), 1000),
		wg:     &sync.WaitGroup{},
		ticker: time.NewTicker(GetTickRate()),
	}

	InitCtx()
	SetName(name)

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
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
		cancel := GetCancel()

		defer signal.Stop(interrupt)
		defer cancel() // Ensure context is cancelled when interrupt is received

		<-interrupt
	}()

	a.Init()
	defer a.Deinit()

	for {
		select {
		case <-GetCtx().Done():
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

func (a *ApplicationImpl) DoInMainThread(t func()) {
	go func() { a.tasks <- t }()
}

func (a *ApplicationImpl) GetWg() *sync.WaitGroup {
	return a.wg
}
