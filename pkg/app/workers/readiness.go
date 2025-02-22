package workers

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

type ReadinessState int

const (
	Ready ReadinessState = iota
	NotReady
)

type ReadinessCriteria interface {
	IsReady() bool
}

type ReadinessCriteriaFunc func() bool

func (f ReadinessCriteriaFunc) IsReady() bool {
	return f()
}

type Readiness struct {
	BecameReady    signalslots.Signal
	BecameNotReady signalslots.Signal

	criterias []ReadinessCriteria
	state     ReadinessState
}

func NewReadiness() *Readiness {
	w := &Readiness{
		BecameReady:    signal.New(),
		BecameNotReady: signal.New(),

		criterias: []ReadinessCriteria{},
		state:     NotReady,
	}

	return w
}

func (w *Readiness) AddCriteria(c ReadinessCriteria) {
	w.criterias = append(w.criterias, c)
}

func (w *Readiness) RemoveCriteria(c ReadinessCriteria) {
	for i, criteria := range w.criterias {
		if criteria == c {
			w.criterias = append(w.criterias[:i], w.criterias[i+1:]...)
			return
		}
	}
}

func (w *Readiness) Init(ctx context.Context, h app.Handle) {

}

func (w *Readiness) Deinit(ctx context.Context) {

}

func (w *Readiness) DoWork(ctx context.Context) {
	if w.IsReady() {
		w.setState(Ready)
	} else {
		w.setState(NotReady)
	}
}

func (w *Readiness) IsReady() bool {
	for _, criteria := range w.criterias {
		if !criteria.IsReady() {
			return false
		}
	}

	return true
}

func (w *Readiness) GetState() ReadinessState {
	return w.state
}

func (w *Readiness) setState(state ReadinessState) {
	if w.state == state {
		return
	}

	w.state = state

	if state == Ready {
		w.BecameReady.Emit()
	} else {
		w.BecameNotReady.Emit()
	}
}
