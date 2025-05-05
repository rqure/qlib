package qworkers

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
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
	isAuthReady bool
}

func (me *StoreConnectedCriteria) IsReady() bool {
	return me.isConnected && me.isAuthReady
}

func (me *StoreConnectedCriteria) OnStoreConnected(context.Context) {
	me.isConnected = true
}

func (me *StoreConnectedCriteria) OnStoreDisconnected(context.Context) {
	me.isConnected = false
}

func (me *StoreConnectedCriteria) OnAuthReady(context.Context) {
	me.isAuthReady = true
}

func (me *StoreConnectedCriteria) OnAuthNotReady(context.Context) {
	me.isAuthReady = false
}

func NewStoreConnectedCriteria(s Store, r Readiness) ReadinessCriteria {
	c := &StoreConnectedCriteria{
		isConnected: false,
		isAuthReady: false,
	}

	s.Connected().Connect(c.OnStoreConnected)
	s.Disconnected().Connect(c.OnStoreDisconnected)

	s.AuthReady().Connect(c.OnAuthReady)
	s.AuthNotReady().Connect(c.OnAuthNotReady)

	r.BecameReady().Connect(s.OnReady)
	r.BecameNotReady().Connect(s.OnNotReady)

	return c
}

type SchemaValidityCriteria interface {
	ReadinessCriteria
	RegisterEntityFields(entityType qdata.EntityType, fields ...qdata.FieldType) SchemaValidityCriteria
}

type schemaValidityCriteria struct {
	isValid   bool
	validator qdata.EntityFieldValidator
}

func (me *schemaValidityCriteria) IsReady() bool {
	return me.isValid
}

func (me *schemaValidityCriteria) RegisterEntityFields(entityType qdata.EntityType, fields ...qdata.FieldType) SchemaValidityCriteria {
	me.validator.RegisterEntityFields(entityType, fields...)
	return me
}

func (me *schemaValidityCriteria) onSchemaChanged(args SchemaChangedArgs) {
	err := me.validator.ValidateFields(args.Ctx)
	if err != nil {
		qlog.Warn("Schema validation failed: %v", err)
		me.isValid = false
	} else {
		qlog.Info("Schema validation succeeded")
		me.isValid = true
	}
}

func NewSchemaValidityCriteria(storeWorker Store, store *qdata.Store) SchemaValidityCriteria {
	c := &schemaValidityCriteria{
		isValid:   false,
		validator: qdata.NewEntityFieldValidator(store),
	}

	storeWorker.SchemaChanged().Connect(c.onSchemaChanged)

	return c
}

type Readiness interface {
	qapp.Worker
	AddCriteria(c ReadinessCriteria)
	RemoveCriteria(c ReadinessCriteria)
	GetState() ReadinessState
	IsReady() bool
	BecameReady() qss.Signal[context.Context]
	BecameNotReady() qss.Signal[context.Context]
}

type readinessWorker struct {
	becameReady    qss.Signal[context.Context]
	becameNotReady qss.Signal[context.Context]

	criterias []ReadinessCriteria
	state     ReadinessState
}

func NewReadiness() Readiness {
	w := &readinessWorker{
		becameReady:    qss.New[context.Context](),
		becameNotReady: qss.New[context.Context](),

		criterias: []ReadinessCriteria{},
		state:     NotReady,
	}

	return w
}

func (me *readinessWorker) BecameReady() qss.Signal[context.Context] {
	return me.becameReady
}

func (me *readinessWorker) BecameNotReady() qss.Signal[context.Context] {
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
		me.setState(ctx, Ready)
	} else {
		me.setState(ctx, NotReady)
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

func (me *readinessWorker) setState(ctx context.Context, state ReadinessState) {
	if me.state == state {
		return
	}

	me.state = state

	if state == Ready {
		qlog.Info("Application status changed to [READY]")
		me.becameReady.Emit(ctx)
	} else {
		qlog.Info("Application status changed to [NOT READY]")
		me.becameNotReady.Emit(ctx)
	}
}
