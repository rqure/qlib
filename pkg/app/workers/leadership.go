package workers

import (
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/leadership"
	"github.com/rqure/qlib/pkg/leadership/candidate"
	"github.com/rqure/qlib/pkg/signalslots"
)

// Modify Leadership struct
type Leadership struct {
	StoreConnectedSlot    signalslots.Slot[any]
	StoreDisconnectedSlot signalslots.Slot[any]

	store            data.Store
	isStoreConnected bool

	handle    app.Handle
	candidate leadership.Candidate

	ticker *time.Ticker
}

// Update initialization
func NewLeaderElectionWorker(store data.Store) *Leadership {
	w := &Leadership{
		StoreConnectedSlot:    signalslots.NewSlot[any](),
		StoreDisconnectedSlot: signalslots.NewSlot[any](),
		store:                 store,
		isStoreConnected:      false,
		candidate:             candidate.New(store),
		ticker:                time.NewTicker(100 * time.Millisecond),
	}

	return w
}

func (w *Leadership) AddAvailabilityCriteria(criteria leadership.AvailabilityCriteria) {
	w.candidate.AddAvailabilityCriteria(criteria)
}

func (w *Leadership) Init(h app.Handle) {
	w.handle = h

	w.candidate.Init()

	w.AddAvailabilityCriteria(func() bool {
		return w.isStoreConnected
	})
}

func (w *Leadership) Deinit() {
	w.candidate.Deinit()
}

func (w *Leadership) DoWork() {
	w.handle.GetWg().Add(1)
	defer w.handle.GetWg().Done()

	for {
		select {
		case <-w.handle.GetCtx().Done():
			return
		case <-w.ticker.C:
			w.handle.Do(func() {
				w.candidate.DoWork()
			})
		case <-w.StoreConnectedSlot:
			w.handle.Do(func() {
				w.isStoreConnected = !w.isStoreConnected
			})
		case <-w.StoreDisconnectedSlot:
			w.handle.Do(func() {
				w.isStoreConnected = !w.isStoreConnected
			})
		}
	}
}
