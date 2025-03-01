package store

import (
	"context"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/auth"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/log"
)

type SessionProvider interface {
	data.SessionProvider
}

type sessionProvider struct {
	core    auth.Core
	session auth.Session
}

func NewSessionProvider() SessionProvider {
	return &sessionProvider{
		core: auth.NewCore(),
	}
}

func (me *sessionProvider) Session(ctx context.Context) auth.Session {
	if me.session == nil || !me.session.IsValid(ctx) {
		admin := auth.NewAdmin(me.core)
		client, err := admin.GetOrCreateClient(ctx, app.GetName())

		if err != nil {
			log.Error("Failed to get or create client: %v", err)
			me.session = auth.NewSession(me.core, nil, "", "", "")
			return me.session
		}

		me.session, err = client.CreateSession(ctx)
		if err != nil {
			log.Error("Failed to create session: %v", err)
			me.session = auth.NewSession(me.core, nil, "", "", "")
			return me.session
		}
	}

	return me.session
}
