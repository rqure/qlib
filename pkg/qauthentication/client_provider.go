package qauthentication

import (
	"context"

	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qlog"
)

type clientProvider struct {
	core   Core
	client Client
}

func NewClientProvider() qcontext.ClientProvider[Client] {
	return &clientProvider{
		core: NewCore(),
	}
}

func (me *clientProvider) Client(ctx context.Context) Client {
	if me.client == nil {
		admin := NewAdmin(me.core)

		// logout of the session after attempting to get the client
		defer admin.Session(ctx).Revoke(ctx)

		client, err := admin.GetOrCreateClient(ctx, qcontext.GetAppName(ctx))

		if err != nil {
			qlog.Warn("Failed to get or create client: %v", err)
			return nil
		}

		me.client = client
	}

	return me.client
}
