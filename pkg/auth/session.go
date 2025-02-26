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

	ExpiresAt() int64
	IsExpired() bool
}

type session struct {
	core  Core
	token *gocloak.JWT
}

func NewSession(core Core, token *gocloak.JWT) Session {
	return &session{
		core:  core,
		token: token,
	}
}

func (s *session) Refresh() error {
	token, err := s.core.GetClient().RefreshToken(context.Background(), s.token.RefreshToken,
		s.token.ClientID, s.token.ClientSecret, "qcore-realm")
	if err != nil {
		return err
	}

	s.token = token
	return nil
}

func (s *session) Revoke() error {
	return s.core.GetClient().Logout(context.Background(), s.token.ClientID,
		s.token.ClientSecret, "qcore-realm", s.token.RefreshToken)
}

func (s *session) AccessToken() string {
	return s.token.AccessToken
}

func (s *session) ExpiresAt() int64 {
	return s.token.ExpiresIn
}

func (s *session) IsExpired() bool {
	// Check if token has expired
	return time.Now().Unix() >= s.token.ExpiresIn
}
