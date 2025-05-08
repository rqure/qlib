package qauthentication

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Nerzal/gocloak/v13"
	"github.com/rqure/qlib/pkg/qcontext"
	"github.com/rqure/qlib/pkg/qlog"
	"github.com/rqure/qlib/pkg/qss"
)

type Session interface {
	Refresh(context.Context) error
	Revoke(context.Context) error

	PastHalfLife(context.Context) bool
	GetOwnerName(context.Context) (string, error)
	IsValid() bool

	AccessToken() string
	RefreshToken() string
	Realm() string
	ClientID() string
	ClientSecret() string

	// New methods for automatic refresh
	StartAutoRefresh(ctx context.Context) error
	StopAutoRefresh()

	Valid() qss.Signal[bool]
}

type session struct {
	core         Core
	token        *gocloak.JWT
	clientID     string
	clientSecret string
	realm        string

	isValid     bool
	validSignal qss.Signal[bool]

	// Auto-refresh related fields
	autoRefreshCtx    context.Context
	autoRefreshCancel context.CancelFunc
	autoRefreshActive bool
	autoRefreshMutex  sync.Mutex
}

func NewSession(core Core, token *gocloak.JWT, clientID, clientSecret, realm string) Session {
	return &session{
		core:         core,
		token:        token,
		clientID:     clientID,
		clientSecret: clientSecret,
		realm:        realm,
		isValid:      false,
		validSignal:  qss.New[bool](),
	}
}

func (me *session) setIsValid(ctx context.Context, isValid bool) {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.isValid != isValid {
		me.isValid = isValid

		handle := qcontext.GetHandle(ctx)
		handle.DoInMainThread(func(ctx context.Context) {
			me.validSignal.Emit(isValid)
		})
	}
}

func (me *session) Valid() qss.Signal[bool] {
	return me.validSignal
}

func (me *session) IsValid() bool {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()
	return me.isValid
}

func (me *session) Refresh(ctx context.Context) error {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.token == nil {
		return fmt.Errorf("no token to refresh")
	}

	token, err := me.core.GetClient().RefreshToken(
		ctx,
		me.token.RefreshToken,
		me.clientID,
		me.clientSecret,
		me.realm,
	)
	if err != nil {
		if strings.Contains(err.Error(), "invalid_grant") {
			token, err = me.core.GetClient().GetToken(
				ctx,
				me.realm,
				gocloak.TokenOptions{
					ClientID:     &me.clientID,
					ClientSecret: &me.clientSecret,
					GrantType:    gocloak.StringP("client_credentials"),
				},
			)
		}

		if err != nil {
			return fmt.Errorf("failed to refresh token: %v", err)
		}
	}

	me.token = token
	return nil
}

func (me *session) Revoke(ctx context.Context) error {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.token == nil {
		return fmt.Errorf("no token to revoke")
	}

	_, claims, err := me.core.GetClient().DecodeAccessToken(ctx, me.token.AccessToken, me.realm)
	if err != nil {
		return err
	}

	sid, ok := (*claims)["sid"].(string)
	if !ok {
		return fmt.Errorf("no session id found in token claims")
	}

	err = me.core.GetClient().LogoutUserSession(ctx, me.token.AccessToken, me.realm, sid)
	if err != nil {
		return err
	}

	me.token = nil
	return nil
}

func (me *session) AccessToken() string {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.token == nil {
		return ""
	}

	return me.token.AccessToken
}

func (me *session) RefreshToken() string {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.token == nil {
		return ""
	}

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
func (me *session) GetOwnerName(ctx context.Context) (string, error) {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.token == nil {
		return "", fmt.Errorf("no token to get owner name")
	}

	_, claims, err := me.core.GetClient().DecodeAccessToken(
		ctx,
		me.token.AccessToken,
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
func (me *session) checkIsValid(ctx context.Context) bool {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if me.token == nil {
		return false
	}

	_, claims, err := me.core.GetClient().DecodeAccessToken(
		ctx,
		me.token.AccessToken,
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

// Check if the token has passed the half-life time
func (me *session) PastHalfLife(ctx context.Context) bool {
	// No need to lock here since this is only called from already locked contexts
	// or from refreshRoutine where we're already handling the locking

	if me.token == nil {
		return false
	}

	_, claims, err := me.core.GetClient().DecodeAccessToken(
		ctx,
		me.token.AccessToken,
		me.realm,
	)
	if err != nil {
		return false
	}

	// Get expiration and issued at times
	exp, expOk := (*claims)["exp"].(float64)
	iat, iatOk := (*claims)["iat"].(float64)

	if !expOk || !iatOk {
		// If we can't get both timestamps, default to false
		return false
	}

	expirationTime := time.Unix(int64(exp), 0)
	issuedAtTime := time.Unix(int64(iat), 0)

	// Calculate total lifetime and midpoint
	totalLifetime := expirationTime.Sub(issuedAtTime)
	midpoint := issuedAtTime.Add(totalLifetime / 2)

	// If current time is past the midpoint, we're in the second half of the token's lifetime
	return time.Now().After(midpoint)
}

// StartAutoRefresh begins a background goroutine that automatically refreshes
// the session token before it expires
func (me *session) StartAutoRefresh(ctx context.Context) error {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	// Don't start a new refresh routine if one is already active
	if me.autoRefreshActive {
		return nil
	}

	// Create a new context with cancellation for the refresh routine
	me.autoRefreshCtx, me.autoRefreshCancel = context.WithCancel(ctx)
	me.autoRefreshActive = true

	// Start the refresh goroutine
	go me.refreshRoutine()

	qlog.Trace("Session auto-refresh started for client %s", me.clientID)
	return nil
}

// StopAutoRefresh stops the background refresh goroutine
func (me *session) StopAutoRefresh() {
	me.autoRefreshMutex.Lock()
	defer me.autoRefreshMutex.Unlock()

	if !me.autoRefreshActive {
		return
	}

	// Cancel the context to stop the refresh routine
	if me.autoRefreshCancel != nil {
		me.autoRefreshCancel()
		me.autoRefreshCancel = nil
	}

	me.autoRefreshActive = false
	qlog.Trace("Session auto-refresh stopped for client %s", me.clientID)
}

// refreshRoutine runs in the background and refreshes the token when needed
func (me *session) refreshRoutine() {
	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	defer ticker.Stop()

	for {
		select {
		case <-me.autoRefreshCtx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(me.autoRefreshCtx, 5*time.Second)

			shouldRefresh := false

			me.autoRefreshMutex.Lock()
			if me.token != nil && me.autoRefreshActive {
				shouldRefresh = me.PastHalfLife(refreshCtx)
			}

			if shouldRefresh {
				if me.token != nil && me.autoRefreshActive {
					err := me.Refresh(refreshCtx)
					if err != nil {
						qlog.Warn("Background session refresh failed: %v", err)
					} else {
						qlog.Trace("Background session refresh succeeded for client %s", me.clientID)
					}
				}
			}

			me.autoRefreshMutex.Unlock()
			cancel()

			me.setIsValid(refreshCtx, me.checkIsValid(refreshCtx))
		}
	}
}
