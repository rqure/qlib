package qstore

import (
	"context"

	"github.com/rqure/qlib/pkg/qapp"
	"github.com/rqure/qlib/pkg/qauth"
	"github.com/rqure/qlib/pkg/qdata"
	"github.com/rqure/qlib/pkg/qlog"
)

type AuthProvider interface {
	qdata.AuthProvider
}

type authProvider struct {
	core   qauth.Core
	client qauth.Client
}

func NewAuthProvider() AuthProvider {
	return &authProvider{
		core: qauth.NewCore(),
	}
}

func (me *authProvider) AuthClient(ctx context.Context) qauth.Client {
	if me.client == nil {
		admin := qauth.NewAdmin(me.core)
		client, err := admin.GetOrCreateClient(ctx, qapp.GetName())

		if err != nil {
			qlog.Error("Failed to get or create client: %v", err)
			return nil
		}

		me.client = client
	}

	return me.client
}
