package auth

import (
	"context"
	"time"

	"github.com/Nerzal/gocloak/v13"
)

type Session interface {
	Refresh() error
	Revoke() error

	AccessToken() string
	RefreshToken() string
	Realm() string
	ClientID() string
	ClientSecret() string

	GetOwnerName() (string, error)

	IsValid() bool
}

type session struct {
	core         Core
	token        *gocloak.JWT
	clientID     string
	clientSecret string
	realm        string
}

func NewSession(core Core, token *gocloak.JWT, clientID, clientSecret, realm string) Session {
	if realm == "" {
		realm = getEnvOrDefault("Q_KEYCLOAK_REALM", "qcore-realm")
	}

	return &session{
		core:         core,
		token:        token,
		clientID:     clientID,
		clientSecret: clientSecret,
		realm:        realm,
	}
}

// clientSession implementation
func (me *session) Refresh() error {
	token, err := me.core.GetClient().RefreshToken(
		context.Background(),
		me.token.RefreshToken,
		me.clientID,
		me.clientSecret,
		me.realm,
	)
	if err != nil {
		return err
	}

	me.token = token
	return nil
}

func (me *session) Revoke() error {
	return me.core.GetClient().Logout(
		context.Background(),
		me.clientID,
		me.clientSecret,
		me.realm,
		me.token.RefreshToken,
	)
}

func (me *session) AccessToken() string {
	return me.token.AccessToken
}

func (me *session) RefreshToken() string {
	return me.token.RefreshToken
}

func (me *session) Realm() string {
	return me.realm
}

func (me *session) ClientID() string {
	return me.clientID
}

func (me *session) ClientSecret() string {
	return me.clientSecret
}

// Returns the name of the owner of the session
// This is the user who owns the session or the client if it's a client session
func (me *session) GetOwnerName() (string, error) {
	ctx := context.Background()
	_, claims, err := me.core.GetClient().DecodeAccessToken(
		ctx,
		me.AccessToken(),
		me.realm,
	)
	if err != nil {
		return "", err
	}

	// For client credentials flow, check for client_id claim
	if clientID, ok := (*claims)["client_id"].(string); ok {
		// If azp (authorized party) is the same as client_id, this is likely a client credentials flow
		if azp, exists := (*claims)["azp"].(string); exists && azp == clientID {
			return clientID, nil
		}
	}

	// Check for preferred_username claim, which is commonly used for the user's name
	if username, ok := (*claims)["preferred_username"].(string); ok {
		return username, nil
	}

	// Fallback to sub claim if preferred_username is not available
	if sub, ok := (*claims)["sub"].(string); ok {
		return sub, nil
	}

	return "", nil
}

// Decode the access token and check if it's still valid
func (me *session) IsValid() bool {
	ctx := context.Background()
	_, claims, err := me.core.GetClient().DecodeAccessToken(
		ctx,
		me.AccessToken(),
		me.realm,
	)
	if err != nil {
		return false
	}

	// Check if token has expired
	if exp, ok := (*claims)["exp"].(float64); ok {
		expirationTime := time.Unix(int64(exp), 0)
		return time.Now().Before(expirationTime)
	}

	return false
}
