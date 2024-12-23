package workers

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/leadership"
	"github.com/rqure/qlib/pkg/leadership/candidate"
	"github.com/rqure/qlib/pkg/signalslots"
)

// Modify Leadership struct
type Leadership struct {
	store            data.Store
	isStoreConnected bool

	storeValidator data.EntityFieldValidator
	isStoreValid   bool

	notificationTokens []data.NotificationToken

	candidate leadership.Candidate

	handle app.Handle
}

// Update initialization
func NewLeadership(store data.Store) *Leadership {
	w := &Leadership{
		store:            store,
		isStoreConnected: false,

		storeValidator: data.NewEntityFieldValidator(store),
		isStoreValid:   false,

		notificationTokens: []data.NotificationToken{},

		candidate: candidate.New(store),
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
		return w.isStoreConnected
	})

	w.AddAvailabilityCriteria(func() bool {
		return w.isStoreValid
	})
}

func (w *Leadership) Deinit(ctx context.Context) {
	w.candidate.Deinit(ctx)
}

func (w *Leadership) DoWork(ctx context.Context) {
	if w.isStoreConnected {
		w.candidate.DoWork(ctx)
	}
}

func (w *Leadership) OnStoreConnected(ctx context.Context) {
	w.isStoreConnected = true

	for _, token := range w.notificationTokens {
		token.Unbind(ctx)
	}

	w.notificationTokens = []data.NotificationToken{}

	w.notificationTokens = append(w.notificationTokens, w.store.Notify(
		ctx,
		notification.NewConfig().
			SetEntityType("Root").
			SetFieldName("SchemaUpdateTrigger"),
		notification.NewCallback(w.OnSchemaUpdated)))

	w.OnSchemaUpdated(ctx, nil)
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

func (w *Leadership) OnSchemaUpdated(ctx context.Context, n data.Notification) {
	w.isStoreValid = true

	if err := w.storeValidator.ValidateFields(ctx); err != nil {
		w.isStoreValid = false
	}
}

func (w *Leadership) GetEntityFieldValidator() data.EntityFieldValidator {
	return w.storeValidator
}
