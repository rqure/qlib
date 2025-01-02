package workers

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/notification"
)

// Modify Leadership struct
type Leadership struct {
	store            data.Store
	isStoreConnected bool

	storeValidator data.EntityFieldValidator
	isStoreValid   bool

	notificationTokens []data.NotificationToken

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
	}

	return w
}

func (w *Leadership) Init(ctx context.Context, h app.Handle) {
	w.handle = h
}

func (w *Leadership) Deinit(ctx context.Context) {
}

func (w *Leadership) DoWork(ctx context.Context) {
	if w.isStoreConnected {
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

func (w *Leadership) OnSchemaUpdated(ctx context.Context, n data.Notification) {
	w.isStoreValid = true

	if err := w.storeValidator.ValidateFields(ctx); err != nil {
		w.isStoreValid = false
	}
}

func (w *Leadership) GetEntityFieldValidator() data.EntityFieldValidator {
	return w.storeValidator
}
