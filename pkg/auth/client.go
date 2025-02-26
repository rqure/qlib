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
}

func NewClient(core Core, id, secret string) Client {
	return &client{
		core:   core,
		id:     id,
		secret: secret,
	}
}

func (c *client) GetID() string {
	return c.id
}

func (c *client) GetSecret() string {
	return c.secret
}

func (c *client) CreateSession(ctx context.Context) (Session, error) {
	token, err := c.core.GetClient().GetToken(ctx, "qcore-realm",
		gocloak.TokenOptions{
			ClientID:     &c.id,
			ClientSecret: &c.secret,
			GrantType:    gocloak.StringP("client_credentials"),
		})
	if err != nil {
		return nil, err
	}

	return NewSession(c.core, token), nil
}

func (c *client) CreateUserSession(ctx context.Context, username, password string) (Session, error) {
	token, err := c.core.GetClient().Login(ctx, c.id, c.secret, "qcore-realm", username, password)
	if err != nil {
		return nil, err
	}

	return NewSession(c.core, token), nil
}
