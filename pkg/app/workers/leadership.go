package workers

import (
	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/leadership"
	"github.com/rqure/qlib/pkg/leadership/candidate"
)

// Modify LeaderElectionWorker struct
type LeaderElectionWorker struct {
	store            data.Store
	isStoreConnected bool

	handle    app.Handle
	candidate leadership.Candidate
}

// Update initialization
func NewLeaderElectionWorker(store data.Store) *LeaderElectionWorker {
	w := &LeaderElectionWorker{
		store:     store,
		candidate: candidate.New(store),
	}

	return w
}

func (w *LeaderElectionWorker) AddAvailabilityCriteria(criteria leadership.AvailabilityCriteria) {
	w.candidate.AddAvailabilityCriteria(criteria)
}

func (w *LeaderElectionWorker) Init(h app.Handle) {
	w.handle = h

	w.candidate.Init()

	w.AddAvailabilityCriteria(func() bool {
		return w.isStoreConnected
	})
}

func (w *LeaderElectionWorker) Deinit() {
	w.candidate.Deinit()
}

func (w *LeaderElectionWorker) OnDatabaseConnected() {
	w.isStoreConnected = true
}

func (w *LeaderElectionWorker) OnDatabaseDisconnected() {
	w.isStoreConnected = false
}

func (w *LeaderElectionWorker) OnSchemaUpdated() {

}

func (w *LeaderElectionWorker) DoWork() {
	w.currentState.DoWork(w)
}
