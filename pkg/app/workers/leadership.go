package workers

import (
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/leadership"
	"github.com/rqure/qlib/pkg/leadership/candidate"
	"github.com/rqure/qlib/pkg/signalslots"
)

// Modify Leadership struct
type Leadership struct {
	store            data.Store
	isStoreConnected bool
	candidate        leadership.Candidate
}

// Update initialization
func NewLeadership(store data.Store) *Leadership {
	w := &Leadership{
		store:            store,
		isStoreConnected: false,
		candidate:        candidate.New(store),
	}

	return w
}

func (w *Leadership) AddAvailabilityCriteria(criteria leadership.AvailabilityCriteria) {
	w.candidate.AddAvailabilityCriteria(criteria)
}

func (w *Leadership) Init(h app.Handle) {
	w.candidate.Init()

	w.AddAvailabilityCriteria(func() bool {
		return w.isStoreConnected
	})
}

func (w *Leadership) Deinit() {
	w.candidate.Deinit()
}

func (w *Leadership) DoWork() {
	w.candidate.DoWork()
}

func (w *Leadership) OnStoreConnected() {
	w.isStoreConnected = true
}

func (w *Leadership) OnStoreDisconnected() {
	w.isStoreConnected = false
}

func (w *Leadership) BecameLeader() signalslots.Signal {
	return w.candidate.BecameLeader()
}

func (w *Leadership) BecameFollower() signalslots.Signal {
	return w.candidate.BecameFollower()
}

func (w *Leadership) BecameUnavailable() signalslots.Signal {
	return w.candidate.BecameUnavailable()
}

func (w *Leadership) LosingLeadership() signalslots.Signal {
	return w.candidate.LosingLeadership()
}
