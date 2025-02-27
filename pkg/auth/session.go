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

	ExpiresAt() time.Time
	IsExpired() bool
}

type session struct {
	core         Core
	token        *gocloak.JWT
	clientID     string
	clientSecret string
	realm        string
	creationTime time.Time
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
		creationTime: time.Now(),
	}
}

// clientSession implementation
func (s *session) Refresh() error {
	token, err := s.core.GetClient().RefreshToken(
		context.Background(),
		s.token.RefreshToken,
		s.clientID,
		s.clientSecret,
		s.realm,
	)
	if err != nil {
		return err
	}

	s.token = token
	s.creationTime = time.Now() // Update creation time on refresh
	return nil
}

func (s *session) Revoke() error {
	return s.core.GetClient().Logout(
		context.Background(),
		s.clientID,
		s.clientSecret,
		s.realm,
		s.token.RefreshToken,
	)
}

func (s *session) AccessToken() string {
	return s.token.AccessToken
}

func (s *session) ExpiresAt() time.Time {
	// Calculate expiry time based on creation time and token's ExpiresIn value
	return s.creationTime.Add(time.Duration(s.token.ExpiresIn) * time.Second)
}

func (s *session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt())
}

func (s *session) RefreshToken() string {
	return s.token.RefreshToken
}

func (s *session) Realm() string {
	return s.realm
}

func (s *session) ClientID() string {
	return s.clientID
}

func (s *session) ClientSecret() string {
	return s.clientSecret
}
