package qauthentication

import (
	"context"
	"fmt"
	"time"

	"github.com/Nerzal/gocloak/v13"
	"github.com/rqure/qlib/pkg/qss"
)

type EventType string

const (
	// Login Events
	EventLogin       EventType = "LOGIN"
	EventLoginError  EventType = "LOGIN_ERROR"
	EventLogout      EventType = "LOGOUT"
	EventLogoutError EventType = "LOGOUT_ERROR"

	// Registration Events
	EventRegister      EventType = "REGISTER"
	EventRegisterError EventType = "REGISTER_ERROR"
	EventDeleteAccount EventType = "DELETE_ACCOUNT"
	EventDeleteError   EventType = "DELETE_ACCOUNT_ERROR"

	// User Profile Events
	EventUpdateProfile  EventType = "UPDATE_PROFILE"
	EventUpdatePassword EventType = "UPDATE_PASSWORD"
	EventUpdateEmail    EventType = "UPDATE_EMAIL"
	EventVerifyEmail    EventType = "VERIFY_EMAIL"
	EventUpdateLocale   EventType = "UPDATE_LOCALE"

	// Identity Provider Events
	EventLinkIDP       EventType = "IDENTITY_PROVIDER_LINK"
	EventUnlinkIDP     EventType = "IDENTITY_PROVIDER_UNLINK"
	EventLoginIDP      EventType = "IDENTITY_PROVIDER_LOGIN"
	EventFirstLoginIDP EventType = "IDENTITY_PROVIDER_FIRST_LOGIN"
	EventPostLoginIDP  EventType = "IDENTITY_PROVIDER_POST_LOGIN"
	EventResponseIDP   EventType = "IDENTITY_PROVIDER_RESPONSE"

	// 2FA/MFA Events
	EventRemoveTotp     EventType = "REMOVE_TOTP"
	EventUpdateTotp     EventType = "UPDATE_TOTP"
	EventGrantConsent   EventType = "GRANT_CONSENT"
	EventUpdateConsent  EventType = "UPDATE_CONSENT"
	EventRevokeConsent  EventType = "REVOKE_CONSENT"
	EventCodeToToken    EventType = "CODE_TO_TOKEN"
	EventCustomRequired EventType = "CUSTOM_REQUIRED_ACTION"

	// Client Events
	EventClientLogin     EventType = "CLIENT_LOGIN"
	EventClientLogout    EventType = "CLIENT_LOGOUT"
	EventClientRegister  EventType = "CLIENT_REGISTER"
	EventClientDelete    EventType = "CLIENT_DELETE"
	EventClientUpdate    EventType = "CLIENT_UPDATE"
	EventClientInfo      EventType = "CLIENT_INFO"
	EventClientInitLogin EventType = "CLIENT_INITIATED_ACCOUNT_LINKING"

	// Token Events
	EventTokenRefresh    EventType = "REFRESH_TOKEN"
	EventTokenExchange   EventType = "TOKEN_EXCHANGE"
	EventIntrospectToken EventType = "INTROSPECT_TOKEN"
	EventValidateToken   EventType = "VALIDATE_ACCESS_TOKEN"

	// Role Events
	EventGrantRole  EventType = "GRANT_ROLE"
	EventRemoveRole EventType = "REMOVE_ROLE"
	EventUpdateRole EventType = "UPDATE_ROLE"

	// Permission Events
	EventPermissionGrant  EventType = "PERMISSION_GRANT"
	EventPermissionRevoke EventType = "PERMISSION_REVOKE"

	// Group Events
	EventGroupMembership EventType = "GROUP_MEMBERSHIP"
	EventJoinGroup       EventType = "JOIN_GROUP"
	EventLeaveGroup      EventType = "LEAVE_GROUP"

	// Session Events
	EventRestart              EventType = "RESTART_AUTHENTICATION"
	EventImpersonate          EventType = "IMPERSONATE"
	EventBackchannelLogin     EventType = "BACKCHANNEL_LOGIN"
	EventBackchannelLogout    EventType = "BACKCHANNEL_LOGOUT"
	EventClientSessionExpired EventType = "CLIENT_SESSION_EXPIRED"
	EventSessionExpired       EventType = "SESSION_EXPIRED"
)

type Event interface {
	Time() time.Time
	Type() EventType
	RealmID() string
	ClientID() string
	UserID() string
	SessionID() string
	IPAddress() string
	Error() string
	Details() map[string]string
}

type keycloakEvent struct {
	time      time.Time
	eventType EventType
	realmID   string
	clientID  string
	userID    string
	sessionID string
	ipAddress string
	errorStr  string
	details   map[string]string
}

func (e *keycloakEvent) Time() time.Time {
	return e.time
}

func (e *keycloakEvent) Type() EventType {
	return e.eventType
}

func (e *keycloakEvent) RealmID() string {
	return e.realmID
}

func (e *keycloakEvent) ClientID() string {
	return e.clientID
}

func (e *keycloakEvent) UserID() string {
	return e.userID
}

func (e *keycloakEvent) SessionID() string {
	return e.sessionID
}

func (e *keycloakEvent) IPAddress() string {
	return e.ipAddress
}

func (e *keycloakEvent) Error() string {
	return e.errorStr
}

func (e *keycloakEvent) Details() map[string]string {
	return e.details
}

type EmittedEvent struct {
	Ctx   context.Context
	Event Event
}

type EventEmitter interface {
	Signal() qss.Signal[EmittedEvent]

	GetLastEventTime() time.Time
	SetLastEventTime(time.Time)
	ProcessNextBatch(ctx context.Context, session Session) error
}

type eventEmitterConfig struct {
	batchSize  int
	eventTypes []EventType
}

type EventEmitterOption func(*eventEmitterConfig)

type eventEmitter struct {
	core          Core
	signal        qss.Signal[EmittedEvent]
	config        *eventEmitterConfig
	lastEventTime time.Time
}

func BatchSize(size int) EventEmitterOption {
	return func(c *eventEmitterConfig) {
		c.batchSize = size
	}
}

func EventTypes(types ...EventType) EventEmitterOption {
	return func(c *eventEmitterConfig) {
		c.eventTypes = types
	}
}

func NewEventEmitter(core Core, opts ...EventEmitterOption) EventEmitter {
	config := &eventEmitterConfig{}

	defaultOpts := []EventEmitterOption{
		BatchSize(100),
	}

	for _, opt := range defaultOpts {
		opt(config)
	}

	for _, opt := range opts {
		opt(config)
	}

	return &eventEmitter{
		core:          core,
		signal:        qss.New[EmittedEvent](),
		config:        config,
		lastEventTime: time.Now(),
	}
}

func (me *eventEmitter) Signal() qss.Signal[EmittedEvent] {
	return me.signal
}

func convertEventTypes(types []EventType) []string {
	if len(types) == 0 {
		return nil
	}
	result := make([]string, len(types))
	for i, t := range types {
		result[i] = string(t)
	}
	return result
}

func convertGocloakEvent(e *gocloak.EventRepresentation) (Event, error) {
	// Convert Unix timestamp to time.Time
	eventTime := time.Unix(e.Time/1000, 0) // Convert milliseconds to seconds

	var eventType string
	if e.Type != nil {
		eventType = *e.Type
	}

	return &keycloakEvent{
		time:      eventTime,
		eventType: EventType(eventType),
		realmID:   gocloak.PString(e.RealmID),
		clientID:  gocloak.PString(e.ClientID),
		userID:    gocloak.PString(e.UserID),
		sessionID: gocloak.PString(e.SessionID),
		ipAddress: gocloak.PString(e.IPAddress),
		errorStr:  "", // Error field might not be available in newer versions
		details:   e.Details,
	}, nil
}

func (me *eventEmitter) ProcessNextBatch(ctx context.Context, session Session) error {
	if session == nil {
		return fmt.Errorf("session is nil")
	}

	if !session.IsValid(ctx) {
		return fmt.Errorf("session is invalid")
	}

	max := int32(me.config.batchSize)
	events, err := me.core.GetClient().GetEvents(ctx, session.AccessToken(), session.Realm(), gocloak.GetEventsParams{
		DateFrom: gocloak.StringP(me.lastEventTime.Format(time.RFC3339)),
		Max:      &max,
		Type:     convertEventTypes(me.config.eventTypes),
	})
	if err != nil {
		return fmt.Errorf("failed to get events: %v", err)
	}

	for _, gocloakEvent := range events {
		event, err := convertGocloakEvent(gocloakEvent)
		if err != nil {
			continue
		}

		if event.Time().After(me.lastEventTime) {
			me.lastEventTime = event.Time()
		}

		me.signal.Emit(EmittedEvent{ctx, event})
	}

	return nil
}

func (me *eventEmitter) GetLastEventTime() time.Time {
	return me.lastEventTime
}

func (me *eventEmitter) SetLastEventTime(t time.Time) {
	me.lastEventTime = t
}
