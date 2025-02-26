package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/Nerzal/gocloak/v13"
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
	EventBackchannelLogin     EventType = "BACKCHANNLE_LOGIN"
	EventClientSessionExpired EventType = "CLIENT_SESSION_EXPIRED"
	EventSessionExpired       EventType = "SESSION_EXPIRED"
)

type EventListener func(context.Context, *KeycloakEvent)

type KeycloakEvent struct {
	Time      time.Time         `json:"time"`
	Type      EventType         `json:"type"`
	RealmID   string            `json:"realmId"`
	ClientID  string            `json:"clientId"`
	UserID    string            `json:"userId,omitempty"`
	SessionID string            `json:"sessionId,omitempty"`
	IPAddress string            `json:"ipAddress"`
	Error     string            `json:"error,omitempty"`
	Details   map[string]string `json:"details,omitempty"`
}

type EventStreamConfig struct {
	// How often to check for new events (default: 5 seconds)
	PollInterval time.Duration
	// How many events to fetch per request (default: 100)
	BatchSize int
	// Which event types to listen for (empty means all)
	EventTypes []EventType
}

func DefaultEventStreamConfig() *EventStreamConfig {
	return &EventStreamConfig{
		PollInterval: 5 * time.Second,
		BatchSize:    100,
		EventTypes:   []EventType{},
	}
}

func (ki *keycloakInteractor) StreamEvents(ctx context.Context, config *EventStreamConfig, listener EventListener) error {
	if config == nil {
		config = DefaultEventStreamConfig()
	}

	ticker := time.NewTicker(config.PollInterval)
	defer ticker.Stop()

	var lastEventTime time.Time

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			max := int32(config.BatchSize)
			events, err := ki.client.GetEvents(ctx, ki.adminToken.AccessToken, realmName, gocloak.GetEventsParams{
				DateFrom: gocloak.StringP(lastEventTime.Format(time.RFC3339)),
				Max:      &max,
				Type:     convertEventTypes(config.EventTypes),
			})
			if err != nil {
				return fmt.Errorf("failed to get events: %v", err)
			}

			for _, event := range events {
				keycloakEvent, err := convertGocloakEvent(event)
				if err != nil {
					continue
				}

				if keycloakEvent.Time.After(lastEventTime) {
					lastEventTime = keycloakEvent.Time
				}

				listener(ctx, keycloakEvent)
			}
		}
	}
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

func convertGocloakEvent(event *gocloak.EventRepresentation) (*KeycloakEvent, error) {
	// Convert Unix timestamp to time.Time
	eventTime := time.Unix(event.Time/1000, 0) // Convert milliseconds to seconds

	var eventType string
	if event.Type != nil {
		eventType = *event.Type
	}

	return &KeycloakEvent{
		Time:      eventTime,
		Type:      EventType(eventType),
		RealmID:   gocloak.PString(event.RealmID),
		ClientID:  gocloak.PString(event.ClientID),
		UserID:    gocloak.PString(event.UserID),
		SessionID: gocloak.PString(event.SessionID),
		IPAddress: gocloak.PString(event.IPAddress),
		Error:     "", // Error field might not be available in newer versions
		Details:   event.Details,
	}, nil
}
