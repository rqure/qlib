package store

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/auth"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
)

type AuthProvider interface {
	data.AuthProvider
}

type authProvider struct {
	core   auth.Core
	client auth.Client
}

func NewAuthProvider() AuthProvider {
	return &authProvider{
		core: auth.NewCore(),
	}
}

func (me *authProvider) AuthClient(ctx context.Context) auth.Client {
	if me.client == nil {
		admin := auth.NewAdmin(me.core)
		client, err := admin.GetOrCreateClient(ctx, app.GetName())

		if err != nil {
			log.Error("Failed to get or create client: %v", err)
			return nil
		}

		me.client = client
	}

	return me.client
}
