package auth

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

type Event interface {
}

type EventEmitter interface {
	Signal() signalslots.Signal
	ProcessNextBatch(ctx context.Context, session Session) error
}

type eventEmitter struct {
	core          Core
	signal        signalslots.Signal
	lastEventTime time.Time
}

func NewEventEmitter(core Core) EventEmitter {
	return &eventEmitter{
		core:          core,
		signal:        signal.New(),
		lastEventTime: time.Now(),
	}
}

func (e *eventEmitter) Signal() signalslots.Signal {
	return e.signal
}

func (e *eventEmitter) ProcessNextBatch(ctx context.Context, session Session) error {
	return nil
}
