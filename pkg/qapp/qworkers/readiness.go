package qworkers

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
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

func (me *StoreConnectedCriteria) OnStoreConnected(context.Context) {
	me.isConnected = true
}

func (me *StoreConnectedCriteria) OnStoreDisconnected(context.Context) {
	me.isConnected = false
}

func NewStoreConnectedCriteria(s Store, r Readiness) ReadinessCriteria {
	c := &StoreConnectedCriteria{
		isConnected: false,
	}

	s.Connected().Connect(c.OnStoreConnected)
	s.Disconnected().Connect(c.OnStoreDisconnected)

	r.BecameReady().Connect(s.OnReady)
	r.BecameNotReady().Connect(s.OnNotReady)

	return c
}

type SchemaValidityCriteria interface {
	ReadinessCriteria
	RegisterEntityFields(entityType string, fields ...string) SchemaValidityCriteria
}

type schemaValidityCriteria struct {
	isValid   bool
	validator qdata.EntityFieldValidator
}

func (me *schemaValidityCriteria) IsReady() bool {
	return me.isValid
}

func (me *schemaValidityCriteria) RegisterEntityFields(entityType string, fields ...string) SchemaValidityCriteria {
	me.validator.RegisterEntityFields(entityType, fields...)
	return me
}

func (me *schemaValidityCriteria) OnSchemaUpdated(ctx context.Context) {
	me.isValid = true

	if err := me.validator.ValidateFields(ctx); err != nil {
		me.isValid = false
	}
}

func NewSchemaValidityCriteria(storeWorker Store, store qdata.Store) SchemaValidityCriteria {
	c := &schemaValidityCriteria{
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
	BecameNotReady() qss.Signal
}

type readinessWorker struct {
	becameReady    qss.Signal
	becameNotReady qss.Signal

	criterias []ReadinessCriteria
	state     ReadinessState
}

func NewReadiness() Readiness {
	w := &readinessWorker{
		becameReady:    qsignal.New(),
		becameNotReady: qsignal.New(),

		criterias: []ReadinessCriteria{},
		state:     NotReady,
	}

	return w
}

func (me *readinessWorker) BecameReady() qss.Signal {
	return me.becameReady
}

func (me *readinessWorker) BecameNotReady() qss.Signal {
	return me.becameNotReady
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
		qlog.Info("Application status changed to [READY]")
		me.becameReady.Emit()
	} else {
		qlog.Info("Application status changed to [NOT READY]")
		me.becameNotReady.Emit()
	}
}
