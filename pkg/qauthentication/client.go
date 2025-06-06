package qauthentication

import (
	"context"

	"github.com/Nerzal/gocloak/v13"
	"github.com/rqure/qlib/pkg/qlog"
)

type Client interface {
	GetID() string
	GetSecret() string
	GetSession(ctx context.Context) Session
	CreateUserSession(ctx context.Context, username, password string) Session
	AccessTokenToSession(ctx context.Context, accessToken string) Session
	RefreshTokenToSession(ctx context.Context, refreshToken string) Session
}

type client struct {
	core    Core
	id      string
	secret  string
	realm   string
	session Session
}

func NewClient(core Core, id, secret, realm string) Client {
	return &client{
		core:   core,
		id:     id,
		secret: secret,
		realm:  realm,
	}
}

func (me *client) GetID() string {
	return me.id
}

func (me *client) GetSecret() string {
	return me.secret
}

func (me *client) GetSession(ctx context.Context) Session {
	if me.session == nil {
		token, err := me.core.GetClient().GetToken(ctx, me.realm,
			gocloak.TokenOptions{
				ClientID:     &me.id,
				ClientSecret: &me.secret,
				GrantType:    gocloak.StringP("client_credentials"),
			})

		if err != nil {
			qlog.Warn("Failed to get token: %v", err)
			me.session = NewSession(me.core, nil, me.id, me.secret, me.realm)
		} else {
			me.session = NewSession(me.core, token, me.id, me.secret, me.realm)
		}
	}

	return me.session
}

func (me *client) AccessTokenToSession(ctx context.Context, accessToken string) Session {
	token := &gocloak.JWT{
		AccessToken: accessToken,
	}

	return NewSession(me.core, token, me.id, me.secret, me.realm)
}

func (me *client) RefreshTokenToSession(ctx context.Context, refreshToken string) Session {
	token := &gocloak.JWT{
		RefreshToken: refreshToken,
	}

	return NewSession(me.core, token, me.id, me.secret, me.realm)
}

func (me *client) CreateUserSession(ctx context.Context, username, password string) Session {
	token, err := me.core.GetClient().Login(ctx, me.id, me.secret, me.realm, username, password)
	if err != nil {
		qlog.Error("Failed to login: %v", err)
		return NewSession(me.core, nil, me.id, me.secret, me.realm)
	}

	return NewSession(me.core, token, me.id, me.secret, me.realm)
}
