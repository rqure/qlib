package auth

// FILE TO BE REMOVED

import (
	"context"
	"fmt"

	"github.com/Nerzal/gocloak/v13"
)

const (
	realmName        = "qcore-realm"
	defaultAdminRole = "admin"
)

type KeycloakInteractor interface {
	InitializeKeycloak(ctx context.Context, defaultClientID string) error

	// Client management
	CreateClient(ctx context.Context, clientID string) (string, error)
	GetClientSecret(ctx context.Context, clientID string) (string, error)
	CreateOrGetClientSecret(ctx context.Context, clientID string) (string, error)

	// Client session management
	CreateClientSession(ctx context.Context, clientID, clientSecret string) (*gocloak.JWT, error)
	ValidateClientSession(ctx context.Context, clientID, accessToken string) (*gocloak.IntroSpectTokenResult, error)

	// User session management
	CreateUserSession(ctx context.Context, clientID, username, password string) (*gocloak.JWT, error)
	ValidateUserSession(ctx context.Context, clientID, accessToken string) (*gocloak.IntroSpectTokenResult, error)
	RefreshUserSession(ctx context.Context, clientID, refreshToken string) (*gocloak.JWT, error)

	// User management
	CreateUser(ctx context.Context, username, email, firstName, lastName, password string) (string, error)
	GetUser(ctx context.Context, userID string) (*UserInfo, error)
	GetUserByUsername(ctx context.Context, username string) (*UserInfo, error)
	UpdateUser(ctx context.Context, userID string, updates map[string]interface{}) error
	AssignRoles(ctx context.Context, userID string, roles []string) error
	RemoveRoles(ctx context.Context, userID string, roles []string) error
	GetUserActiveSessions(ctx context.Context, userID string) (int, error)

	// Event streaming
	StreamEvents(ctx context.Context, config *EventStreamConfig, listener EventListener) error
}

type keycloakInteractor struct {
	client      *gocloak.GoCloak
	adminToken  *gocloak.JWT
	realmConfig *RealmConfig
}

type RealmConfig struct {
	AdminUsername string
	AdminPassword string
	BaseURL       string
	MasterRealm   string
}

// UserInfo represents the user information structure
type UserInfo struct {
	ID             string
	Username       string
	Email          string
	FirstName      string
	LastName       string
	Enabled        bool
	EmailVerified  bool
	Roles          []string
	ActiveSessions int
}

func NewKeycloakInteractor(ctx context.Context, config *RealmConfig) (KeycloakInteractor, error) {
	ki := &keycloakInteractor{
		client:      gocloak.NewClient(config.BaseURL),
		realmConfig: config,
	}

	if err := ki.authenticate(ctx); err != nil {
		return nil, fmt.Errorf("authentication failed: %v", err)
	}

	return ki, nil
}

func (ki *keycloakInteractor) InitializeKeycloak(ctx context.Context, defaultClientID string) error {
	if err := ki.ensureRealmExists(ctx); err != nil {
		return fmt.Errorf("realm setup failed: %v", err)
	}

	if err := ki.ensureClientExists(ctx, defaultClientID); err != nil {
		return fmt.Errorf("client setup failed: %v", err)
	}

	if err := ki.ensureRolesExist(ctx); err != nil {
		return fmt.Errorf("roles setup failed: %v", err)
	}

	return nil
}

func (ki *keycloakInteractor) authenticate(ctx context.Context) error {
	token, err := ki.client.LoginAdmin(ctx, ki.realmConfig.AdminUsername, ki.realmConfig.AdminPassword, ki.realmConfig.MasterRealm)
	if err != nil {
		return err
	}
	ki.adminToken = token
	return nil
}

func (ki *keycloakInteractor) ensureRealmExists(ctx context.Context) error {
	_, err := ki.client.GetRealm(ctx, ki.adminToken.AccessToken, realmName)
	if err != nil {
		// Create realm if it doesn't exist
		realm := &gocloak.RealmRepresentation{
			Realm:   gocloak.StringP(realmName),
			Enabled: gocloak.BoolP(true),
		}
		_, err = ki.client.CreateRealm(ctx, ki.adminToken.AccessToken, *realm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ki *keycloakInteractor) ensureClientExists(ctx context.Context, clientID string) error {
	clients, err := ki.client.GetClients(ctx, ki.adminToken.AccessToken, realmName, gocloak.GetClientsParams{
		ClientID: gocloak.StringP(clientID),
	})
	if err != nil || len(clients) == 0 {
		client := &gocloak.Client{
			ClientID:                  gocloak.StringP(clientID),
			Enabled:                   gocloak.BoolP(true),
			StandardFlowEnabled:       gocloak.BoolP(true),
			DirectAccessGrantsEnabled: gocloak.BoolP(true),
		}
		_, err = ki.client.CreateClient(ctx, ki.adminToken.AccessToken, realmName, *client)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ki *keycloakInteractor) ensureRolesExist(ctx context.Context) error {
	role, err := ki.client.GetRealmRole(ctx, ki.adminToken.AccessToken, realmName, defaultAdminRole)
	if err != nil || role == nil {
		role := &gocloak.Role{
			Name: gocloak.StringP(defaultAdminRole),
		}
		_, err = ki.client.CreateRealmRole(ctx, ki.adminToken.AccessToken, realmName, *role)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ki *keycloakInteractor) CreateClientSession(ctx context.Context, clientID, clientSecret string) (*gocloak.JWT, error) {
	token, err := ki.client.GetToken(ctx, realmName, gocloak.TokenOptions{
		ClientID:     &clientID,
		ClientSecret: &clientSecret,
		GrantType:    gocloak.StringP("client_credentials"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client session: %v", err)
	}
	return token, nil
}

func (ki *keycloakInteractor) ValidateClientSession(ctx context.Context, clientID, accessToken string) (*gocloak.IntroSpectTokenResult, error) {
	result, err := ki.client.RetrospectToken(ctx, accessToken, clientID, "", realmName)
	if err != nil {
		return nil, fmt.Errorf("failed to validate client session: %v", err)
	}
	if !*result.Active {
		return nil, fmt.Errorf("token is not active")
	}
	return result, nil
}

func (ki *keycloakInteractor) CreateUserSession(ctx context.Context, clientID, username, password string) (*gocloak.JWT, error) {
	token, err := ki.client.Login(ctx,
		clientID,
		"",
		realmName,
		username,
		password,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create user session: %v", err)
	}
	return token, nil
}

func (ki *keycloakInteractor) ValidateUserSession(ctx context.Context, clientID, accessToken string) (*gocloak.IntroSpectTokenResult, error) {
	result, err := ki.client.RetrospectToken(ctx, accessToken, clientID, "", realmName)
	if err != nil {
		return nil, fmt.Errorf("failed to validate user session: %v", err)
	}
	if !*result.Active {
		return nil, fmt.Errorf("token is not active")
	}
	return result, nil
}

func (ki *keycloakInteractor) RefreshUserSession(ctx context.Context, clientID, refreshToken string) (*gocloak.JWT, error) {
	token, err := ki.client.RefreshToken(ctx, refreshToken, clientID, "", realmName)
	if err != nil {
		return nil, fmt.Errorf("failed to refresh user session: %v", err)
	}
	return token, nil
}

func (ki *keycloakInteractor) CreateClient(ctx context.Context, clientID string) (string, error) {
	client := &gocloak.Client{
		ClientID:                  gocloak.StringP(clientID),
		Enabled:                   gocloak.BoolP(true),
		StandardFlowEnabled:       gocloak.BoolP(true),
		DirectAccessGrantsEnabled: gocloak.BoolP(true),
		ServiceAccountsEnabled:    gocloak.BoolP(true),
	}

	clientUID, err := ki.client.CreateClient(ctx, ki.adminToken.AccessToken, realmName, *client)
	if err != nil {
		return "", fmt.Errorf("failed to create client: %v", err)
	}

	// Get the client secret
	credential, err := ki.client.GetClientSecret(ctx, ki.adminToken.AccessToken, realmName, clientUID)
	if err != nil {
		return "", fmt.Errorf("failed to get client secret: %v", err)
	}

	return *credential.Value, nil
}

func (ki *keycloakInteractor) GetClientSecret(ctx context.Context, clientID string) (string, error) {
	clients, err := ki.client.GetClients(ctx, ki.adminToken.AccessToken, realmName, gocloak.GetClientsParams{
		ClientID: gocloak.StringP(clientID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to get client: %v", err)
	}

	if len(clients) == 0 {
		return "", fmt.Errorf("client not found: %s", clientID)
	}

	credential, err := ki.client.GetClientSecret(ctx, ki.adminToken.AccessToken, realmName, *clients[0].ID)
	if err != nil {
		return "", fmt.Errorf("failed to get client secret: %v", err)
	}

	return *credential.Value, nil
}

func (ki *keycloakInteractor) CreateOrGetClientSecret(ctx context.Context, clientID string) (string, error) {
	// Try to get existing client secret first
	clients, err := ki.client.GetClients(ctx, ki.adminToken.AccessToken, realmName, gocloak.GetClientsParams{
		ClientID: gocloak.StringP(clientID),
	})

	if err == nil && len(clients) > 0 {
		// Client exists, get its secret
		credential, err := ki.client.GetClientSecret(ctx, ki.adminToken.AccessToken, realmName, *clients[0].ID)
		if err == nil {
			return *credential.Value, nil
		}
	}

	// If we get here, either the client doesn't exist or we couldn't get its secret
	// Try to create a new client
	return ki.CreateClient(ctx, clientID)
}

func (ki *keycloakInteractor) CreateUser(ctx context.Context, username, email, firstName, lastName, password string) (string, error) {
	user := gocloak.User{
		Username:      gocloak.StringP(username),
		Email:         gocloak.StringP(email),
		FirstName:     gocloak.StringP(firstName),
		LastName:      gocloak.StringP(lastName),
		Enabled:       gocloak.BoolP(true),
		EmailVerified: gocloak.BoolP(false),
	}

	userID, err := ki.client.CreateUser(ctx, ki.adminToken.AccessToken, realmName, user)
	if err != nil {
		return "", fmt.Errorf("failed to create user: %v", err)
	}

	// Set password
	err = ki.client.SetPassword(ctx, ki.adminToken.AccessToken, userID, realmName, password, false)
	if err != nil {
		return "", fmt.Errorf("failed to set password: %v", err)
	}

	return userID, nil
}

func (ki *keycloakInteractor) GetUser(ctx context.Context, userID string) (*UserInfo, error) {
	user, err := ki.client.GetUserByID(ctx, ki.adminToken.AccessToken, realmName, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %v", err)
	}

	// Get user roles
	roles, err := ki.client.GetRealmRolesByUserID(ctx, ki.adminToken.AccessToken, realmName, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %v", err)
	}

	// Get active sessions
	sessions, err := ki.client.GetUserSessions(ctx, ki.adminToken.AccessToken, realmName, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user sessions: %v", err)
	}

	roleNames := make([]string, len(roles))
	for i, role := range roles {
		roleNames[i] = *role.Name
	}

	return &UserInfo{
		ID:             *user.ID,
		Username:       *user.Username,
		Email:          *user.Email,
		FirstName:      *user.FirstName,
		LastName:       *user.LastName,
		Enabled:        *user.Enabled,
		EmailVerified:  *user.EmailVerified,
		Roles:          roleNames,
		ActiveSessions: len(sessions),
	}, nil
}

func (ki *keycloakInteractor) GetUserByUsername(ctx context.Context, username string) (*UserInfo, error) {
	params := gocloak.GetUsersParams{
		Username: gocloak.StringP(username),
	}
	users, err := ki.client.GetUsers(ctx, ki.adminToken.AccessToken, realmName, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get user: %v", err)
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	return ki.GetUser(ctx, *users[0].ID)
}

func (ki *keycloakInteractor) UpdateUser(ctx context.Context, userID string, updates map[string]interface{}) error {
	user, err := ki.client.GetUserByID(ctx, ki.adminToken.AccessToken, realmName, userID)
	if err != nil {
		return fmt.Errorf("failed to get user: %v", err)
	}

	// Apply updates
	for key, value := range updates {
		switch key {
		case "email":
			user.Email = gocloak.StringP(value.(string))
		case "firstName":
			user.FirstName = gocloak.StringP(value.(string))
		case "lastName":
			user.LastName = gocloak.StringP(value.(string))
		case "enabled":
			user.Enabled = gocloak.BoolP(value.(bool))
		case "emailVerified":
			user.EmailVerified = gocloak.BoolP(value.(bool))
		}
	}

	err = ki.client.UpdateUser(ctx, ki.adminToken.AccessToken, realmName, *user)
	if err != nil {
		return fmt.Errorf("failed to update user: %v", err)
	}

	return nil
}

func (ki *keycloakInteractor) AssignRoles(ctx context.Context, userID string, roles []string) error {
	for _, roleName := range roles {
		role, err := ki.client.GetRealmRole(ctx, ki.adminToken.AccessToken, realmName, roleName)
		if err != nil {
			return fmt.Errorf("failed to get role %s: %v", roleName, err)
		}

		err = ki.client.AddRealmRoleToUser(ctx, ki.adminToken.AccessToken, realmName, userID, []gocloak.Role{*role})
		if err != nil {
			return fmt.Errorf("failed to assign role %s: %v", roleName, err)
		}
	}
	return nil
}

func (ki *keycloakInteractor) RemoveRoles(ctx context.Context, userID string, roles []string) error {
	for _, roleName := range roles {
		role, err := ki.client.GetRealmRole(ctx, ki.adminToken.AccessToken, realmName, roleName)
		if err != nil {
			return fmt.Errorf("failed to get role %s: %v", roleName, err)
		}

		err = ki.client.DeleteRealmRoleFromUser(ctx, ki.adminToken.AccessToken, realmName, userID, []gocloak.Role{*role})
		if err != nil {
			return fmt.Errorf("failed to remove role %s: %v", roleName, err)
		}
	}
	return nil
}

func (ki *keycloakInteractor) GetUserActiveSessions(ctx context.Context, userID string) (int, error) {
	sessions, err := ki.client.GetUserSessions(ctx, ki.adminToken.AccessToken, realmName, userID)
	if err != nil {
		return 0, fmt.Errorf("failed to get user sessions: %v", err)
	}
	return len(sessions), nil
}
