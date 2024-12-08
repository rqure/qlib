package application

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/signalslots"
)

type ApplicationConfig struct {
	Name    string
	Workers []app.Worker
}

type Application struct {
	config ApplicationConfig

	quit chan interface{}

	deinit signalslots.Signal
	init   signalslots.Signal
	tick   signalslots.Signal
}

func NewApplication(config ApplicationConfig) app.Application {
	a := &Application{
		config: config,
		quit:   make(chan interface{}, 1),
		deinit: signalslots.NewSignal(),
		init:   signalslots.NewSignal(),
		tick:   signalslots.NewSignal(),
	}

	app.SetApplicationName(config.Name)

	for _, worker := range config.Workers {
		a.init.Connect(signalslots.NewSlot(worker.Init))
		a.deinit.Connect(signalslots.NewSlot(worker.Deinit))
		a.tick.Connect(signalslots.NewSlot(worker.DoWork))
	}

	return a
}

func (a *Application) Execute() {
	defer a.init.DisconnectAll()
	defer a.deinit.DisconnectAll()
	defer a.tick.DisconnectAll()

	a.init.Emit()
	defer a.deinit.Emit()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-interrupt:
			return
		case <-ticker.C:
			a.tick.Emit()
		case <-a.quit:
			return
		}
	}
}

func (a *Application) Quit() {
	a.quit <- nil
}
