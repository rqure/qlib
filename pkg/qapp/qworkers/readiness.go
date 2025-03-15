package qworkers

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qss"
	"github.com/rqure/qlib/pkg/qss/qsignal"
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

func (me *StoreConnectedCriteria) IsReady() bool {
	return me.isConnected
}

func (me *StoreConnectedCriteria) OnStoreConnected() {
	me.isConnected = true
}

func (me *StoreConnectedCriteria) OnStoreDisconnected() {
	me.isConnected = false
}

func NewStoreConnectedCriteria(s Store) ReadinessCriteria {
	c := &StoreConnectedCriteria{
		isConnected: false,
	}

	s.Connected().Connect(c.OnStoreConnected)
	s.Disconnected().Connect(c.OnStoreDisconnected)

	return c
}

type SchemaValidityCriteria struct {
	isValid   bool
	validator qdata.EntityFieldValidator
}

func (me *SchemaValidityCriteria) IsReady() bool {
	return me.isValid
}

func (me *SchemaValidityCriteria) RegisterEntityFields(entityType string, fields ...string) {
	me.validator.RegisterEntityFields(entityType, fields...)
}

func (me *SchemaValidityCriteria) OnSchemaUpdated(ctx context.Context) {
	me.isValid = true

	if err := me.validator.ValidateFields(ctx); err != nil {
		me.isValid = false
	}
}

func NewSchemaValidityCriteria(storeWorker Store, store qdata.Store) ReadinessCriteria {
	c := &SchemaValidityCriteria{
		isValid:   false,
		validator: qdata.NewEntityFieldValidator(store),
	}

	storeWorker.SchemaUpdated().Connect(c.OnSchemaUpdated)

	return c
}

type Readiness interface {
	qapp.Worker
	AddCriteria(c ReadinessCriteria)
	RemoveCriteria(c ReadinessCriteria)
	GetState() ReadinessState
	IsReady() bool
	BecameReady() qss.Signal
	BecameUnready() qss.Signal
}

type readinessWorker struct {
	becameReady   qss.Signal
	becameUnready qss.Signal

	criterias []ReadinessCriteria
	state     ReadinessState
}

func NewReadiness() Readiness {
	w := &readinessWorker{
		becameReady:   qsignal.New(),
		becameUnready: qsignal.New(),

		criterias: []ReadinessCriteria{},
		state:     NotReady,
	}

	return w
}

func (me *readinessWorker) BecameReady() qss.Signal {
	return me.becameReady
}

func (me *readinessWorker) BecameUnready() qss.Signal {
	return me.becameUnready
}

func (me *readinessWorker) AddCriteria(c ReadinessCriteria) {
	me.criterias = append(me.criterias, c)
}

func (me *readinessWorker) RemoveCriteria(c ReadinessCriteria) {
	for i, criteria := range me.criterias {
		if criteria == c {
			me.criterias = append(me.criterias[:i], me.criterias[i+1:]...)
			return
		}
	}
}

func (me *readinessWorker) Init(ctx context.Context) {

}

func (me *readinessWorker) Deinit(ctx context.Context) {

}

func (me *readinessWorker) DoWork(ctx context.Context) {
	if me.IsReady() {
		me.setState(Ready)
	} else {
		me.setState(NotReady)
	}
}

func (me *readinessWorker) IsReady() bool {
	for _, criteria := range me.criterias {
		if !criteria.IsReady() {
			return false
		}
	}

	return true
}

func (me *readinessWorker) GetState() ReadinessState {
	return me.state
}

func (me *readinessWorker) setState(state ReadinessState) {
	if me.state == state {
		return
	}

	me.state = state

	if state == Ready {
		me.becameReady.Emit()
	} else {
		me.becameUnready.Emit()
	}
}
