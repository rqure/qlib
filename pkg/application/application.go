package qapp

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

type IApplication interface {
	Execute()
	Quit()
}

type IWorker interface {
	Deinit()
	DoWork()
	Init()
}

type ApplicationConfig struct {
	Name    string
	Workers []IWorker
}

type Application struct {
	config ApplicationConfig

	quit chan interface{}

	deinit Signal
	init   Signal
	tick   Signal
}

var applicationName string

func GetApplicationName() string {
	if applicationName == "" {
		applicationName = os.Getenv("QDB_APP_NAME")
	}

	return applicationName
}

func SetApplicationName(name string) {
	if os.Getenv("QDB_APP_NAME") == "" {
		applicationName = name
	}
}

func NewApplication(config ApplicationConfig) IApplication {
	a := &Application{
		config: config,
		quit:   make(chan interface{}, 1),
	}

	SetApplicationName(config.Name)

	for _, worker := range config.Workers {
		a.init.Connect(Slot(worker.Init))
		a.deinit.Connect(Slot(worker.Deinit))
		a.tick.Connect(Slot(worker.DoWork))
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
