package auth

import (
	"context"

	"github.com/Nerzal/gocloak/v13"
)

type Client interface {
	GetID() string
	GetSecret() string
	CreateSession(ctx context.Context) (Session, error)
	CreateUserSession(ctx context.Context, username, password string) (Session, error)
}

type client struct {
	core   Core
	id     string
	secret string
	realm  string
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

func (me *client) CreateSession(ctx context.Context) (Session, error) {
	token, err := me.core.GetClient().GetToken(ctx, me.realm,
		gocloak.TokenOptions{
			ClientID:     &me.id,
			ClientSecret: &me.secret,
			GrantType:    gocloak.StringP("client_credentials"),
		})
	if err != nil {
		return nil, err
	}

	return NewSession(me.core, token, me.id, me.secret, me.realm), nil
}

func (me *client) CreateUserSession(ctx context.Context, username, password string) (Session, error) {
	token, err := me.core.GetClient().Login(ctx, me.id, me.secret, me.realm, username, password)
	if err != nil {
		return nil, err
	}

	return NewSession(me.core, token, me.id, me.secret, me.realm), nil
}
