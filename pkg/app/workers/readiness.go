package workers

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
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

type StoreConnectedCriteria struct {
	isConnected bool
}

func (c *StoreConnectedCriteria) IsReady() bool {
	return c.isConnected
}

func (c *StoreConnectedCriteria) OnStoreConnected() {
	c.isConnected = true
}

func (c *StoreConnectedCriteria) OnStoreDisconnected() {
	c.isConnected = false
}

func NewStoreConnectedCriteria(s Store) ReadinessCriteria {
	c := &StoreConnectedCriteria{
		isConnected: false,
	}

	s.Connected.Connect(c.OnStoreConnected)
	s.Disconnected.Connect(c.OnStoreDisconnected)

	return c
}

type SchemaValidityCriteria struct {
	isValid   bool
	validator data.EntityFieldValidator
}

func (c *SchemaValidityCriteria) IsReady() bool {
	return c.isValid
}

func (c *SchemaValidityCriteria) OnSchemaUpdated(ctx context.Context) {
	c.isValid = true

	if err := c.validator.ValidateFields(ctx); err != nil {
		c.isValid = false
	}
}

func NewSchemaValidityCriteria(s Store) ReadinessCriteria {
	c := &SchemaValidityCriteria{
		isValid:   false,
		validator: data.NewEntityFieldValidator(s.store),
	}

	s.SchemaUpdated.Connect(c.OnSchemaUpdated)

	return c
}

type Readiness struct {
	BecameReady   signalslots.Signal
	BecameUnready signalslots.Signal

	criterias []ReadinessCriteria
	state     ReadinessState
}

func NewReadiness() *Readiness {
	w := &Readiness{
		BecameReady:   signal.New(),
		BecameUnready: signal.New(),

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
		w.BecameUnready.Emit()
	}
}
