package workers

import (
	"context"
	"os"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/store"
	"github.com/rqure/qlib/pkg/leadership"
	"github.com/rqure/qlib/pkg/leadership/candidate"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
)

func getLeaderStoreAddress() string {
	addr := os.Getenv("Q_LEADER_STORE_ADDR")

	if addr == "" {
		addr = "redis:6379"
	}

	return addr
}

func getLeaderStorePassword() string {
	return os.Getenv("Q_LEADER_STORE_PASSWORD")
}

// Modify Leadership struct
type Leadership struct {
	dataStore            data.Store
	isDataStoreConnected bool

	storeValidator data.EntityFieldValidator
	isStoreValid   bool

	candidate leadership.Candidate

	handle app.Handle

	leaderStore            leadership.Store
	isLeaderStoreConnected bool
	connectionCheckTicker  *time.Ticker
}

// Update initialization
func NewLeadership(ds data.Store) *Leadership {
	ls := store.NewRedis(store.RedisConfig{
		Address:  getLeaderStoreAddress(),
		Password: getLeaderStorePassword(),
	})

	w := &Leadership{
		dataStore:            ds,
		isDataStoreConnected: false,

		leaderStore:            ls,
		isLeaderStoreConnected: false,

		storeValidator: data.NewEntityFieldValidator(ds),
		isStoreValid:   false,

		candidate: candidate.New(ls, ds),

		connectionCheckTicker: time.NewTicker(5 * time.Second),
	}

	return w
}

func (w *Leadership) AddAvailabilityCriteria(criteria leadership.AvailabilityCriteria) {
	w.candidate.AddAvailabilityCriteria(criteria)
}

func (w *Leadership) Init(ctx context.Context, h app.Handle) {
	w.handle = h

	w.candidate.Init(ctx)

	w.AddAvailabilityCriteria(func() bool {
		return w.isLeaderStoreConnected
	})

	w.AddAvailabilityCriteria(func() bool {
		return w.isDataStoreConnected
	})

	w.AddAvailabilityCriteria(func() bool {
		return w.isStoreValid
	})
}

func (w *Leadership) Deinit(ctx context.Context) {
	w.connectionCheckTicker.Stop()
	w.candidate.Deinit(ctx)
}

func (w *Leadership) DoWork(ctx context.Context) {
	select {
	case <-w.connectionCheckTicker.C:
		w.setLeaderStoreConnectionStatus(w.leaderStore.IsConnected(ctx))

		if !w.IsLeaderStoreConnected() {
			w.leaderStore.Connect(ctx)
			w.setLeaderStoreConnectionStatus(w.leaderStore.IsConnected(ctx))
		}
	default:
		if w.isDataStoreConnected && w.isLeaderStoreConnected {
			w.candidate.DoWork(ctx)
		}
	}
}

func (w *Leadership) OnStoreConnected(ctx context.Context) {
	w.isDataStoreConnected = true
}

func (w *Leadership) OnStoreDisconnected() {
	w.isDataStoreConnected = false
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

func (w *Leadership) OnSchemaUpdated(ctx context.Context) {
	w.isStoreValid = true

	if err := w.storeValidator.ValidateFields(ctx); err != nil {
		w.isStoreValid = false
	}
}

func (w *Leadership) GetEntityFieldValidator() data.EntityFieldValidator {
	return w.storeValidator
}

func (w *Leadership) setLeaderStoreConnectionStatus(connected bool) {
	if w.isLeaderStoreConnected == connected {
		return
	}

	w.isLeaderStoreConnected = connected
	if connected {
		log.Info("Leader store connection status changed to [CONNECTED]")
	} else {
		log.Info("Leader store connection status changed to [DISCONNECTED]")
	}
}

func (w *Leadership) IsLeaderStoreConnected() bool {
	return w.isLeaderStoreConnected
}
