package qauthentication

import (
	"context"
	"fmt"

	"github.com/Nerzal/gocloak/v13"
	"github.com/rqure/qlib/pkg/qlog"
)

type Admin interface {
	EnsureSetup(ctx context.Context) error
	GetOrCreateClient(ctx context.Context, clientID string) (Client, error)
	Session(ctx context.Context) Session

	CreateUser(ctx context.Context, username, password string) error
	DeleteUser(ctx context.Context, username string) error
	GetUser(ctx context.Context, username string) (User, error)
	GetUsers(ctx context.Context) (map[string]User, error)
	UpdateUser(ctx context.Context, user User) error
}

type admin struct {
	core    Core
	session Session
	config  *adminConfig
}

type adminConfig struct {
	masterRealm string
	realm       string
	clientID    string
	username    string
	password    string
}

type AdminOption func(*adminConfig)

func MasterRealm(masterRealm string) AdminOption {
	return func(c *adminConfig) {
		c.masterRealm = masterRealm
	}
}

func Realm(realm string) AdminOption {
	return func(c *adminConfig) {
		c.realm = realm
	}
}

func Username(username string) AdminOption {
	return func(c *adminConfig) {
		c.username = username
	}
}

func Password(password string) AdminOption {
	return func(c *adminConfig) {
		c.password = password
	}
}

func ClientID(clientID string) AdminOption {
	return func(c *adminConfig) {
		c.clientID = clientID
	}
}

func NewAdmin(core Core, opts ...AdminOption) Admin {
	config := &adminConfig{}

	defaultOpts := []AdminOption{
		MasterRealm(getEnvOrDefault("Q_KEYCLOAK_MASTER_REALM", "master")),
		Realm(getEnvOrDefault("Q_KEYCLOAK_REALM", "qos")),
		Username(getEnvOrDefault("Q_KEYCLOAK_ADMIN_USER", "admin")),
		Password(getEnvOrDefault("Q_KEYCLOAK_ADMIN_PASSWORD", "admin")),
		ClientID(getEnvOrDefault("Q_KEYCLOAK_ADMIN_CLIENT_ID", "admin-qcore")),
	}

	for _, opt := range defaultOpts {
		opt(config)
	}

	for _, opt := range opts {
		opt(config)
	}

	return &admin{
		core:   core,
		config: config,
	}
}

func (me *admin) EnsureSetup(ctx context.Context) error {
	// Authenticate as admin
	if err := me.authenticate(ctx); err != nil {
		return fmt.Errorf("admin authentication failed: %w", err)
	}

	// Create realm if it doesn't exist
	realms, err := me.core.GetClient().GetRealms(ctx, me.session.AccessToken())
	if err != nil {
		return fmt.Errorf("failed to get realms: %w", err)
	}

	realmExists := false
	for _, r := range realms {
		if *r.Realm == me.config.realm {
			realmExists = true
			break
		}
	}

	if !realmExists {
		realm := gocloak.RealmRepresentation{
			Realm:   gocloak.StringP(me.config.realm),
			Enabled: gocloak.BoolP(true),
		}

		_, err := me.core.GetClient().CreateRealm(ctx, me.session.AccessToken(), realm)
		if err != nil {
			return fmt.Errorf("failed to create realm: %w", err)
		}
	}

	return nil
}

// GetOrCreateClient creates a new client or gets an existing one
func (me *admin) GetOrCreateClient(ctx context.Context, clientID string) (Client, error) {
	if err := me.authenticate(ctx); err != nil {
		return nil, err
	}

	return me.getOrCreateClient(ctx, clientID, me.session.AccessToken(), me.config.realm)
}

// CreateUser creates a new user with the given username and password
func (me *admin) CreateUser(ctx context.Context, username, password string) error {
	if err := me.authenticate(ctx); err != nil {
		return err
	}

	user := gocloak.User{
		Username: &username,
		Enabled:  gocloak.BoolP(true),
	}

	// Create the user
	userID, err := me.core.GetClient().CreateUser(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		user,
	)
	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	// Set the password
	err = me.core.GetClient().SetPassword(
		ctx,
		me.session.AccessToken(),
		userID,
		me.config.realm,
		password,
		false,
	)
	if err != nil {
		return fmt.Errorf("failed to set password: %w", err)
	}

	return nil
}

// DeleteUser deletes the user with the given username
func (me *admin) DeleteUser(ctx context.Context, username string) error {
	if err := me.authenticate(ctx); err != nil {
		return err
	}

	// Find user by username
	users, err := me.core.GetClient().GetUsers(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		gocloak.GetUsersParams{
			Username: &username,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to get users: %w", err)
	}

	if len(users) == 0 {
		return fmt.Errorf("user not found: %s", username)
	}

	// Delete the user
	err = me.core.GetClient().DeleteUser(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		*users[0].ID,
	)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	return nil
}

// GetUser retrieves a user by username
func (me *admin) GetUser(ctx context.Context, username string) (User, error) {
	if err := me.authenticate(ctx); err != nil {
		return nil, err
	}

	// Find user by username
	users, err := me.core.GetClient().GetUsers(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		gocloak.GetUsersParams{
			Username: &username,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get users: %w", err)
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("user not found: %s", username)
	}

	// Get user roles
	kcUser := users[0]

	// Convert to our User type using proper setters
	user := NewUser(kcUser)

	return user, nil
}

// GetUsers retrieves all users
func (me *admin) GetUsers(ctx context.Context) (map[string]User, error) {
	if err := me.authenticate(ctx); err != nil {
		return nil, err
	}

	// Get all users
	kcUsers, err := me.core.GetClient().GetUsers(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		gocloak.GetUsersParams{
			Max: gocloak.IntP(-1),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get users: %w", err)
	}

	// Convert to our User type
	users := make(map[string]User)
	for _, kcUser := range kcUsers {
		if kcUser.Username == nil {
			continue
		}

		user := NewUser(kcUser)

		users[*kcUser.Username] = user
	}

	return users, nil
}

// UpdateUser updates a user's information
func (me *admin) UpdateUser(ctx context.Context, user User) error {
	if err := me.authenticate(ctx); err != nil {
		return err
	}

	// Find user by username
	users, err := me.core.GetClient().GetUsers(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		gocloak.GetUsersParams{
			Username: gocloak.StringP(user.GetUsername()),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to get users: %w", err)
	}

	if len(users) == 0 {
		return fmt.Errorf("user not found: %s", user.GetUsername())
	}

	// Update user details
	kcUser := users[0]
	kcUser.FirstName = gocloak.StringP(user.GetFirstName())
	kcUser.LastName = gocloak.StringP(user.GetLastName())
	kcUser.Email = gocloak.StringP(user.GetEmail())
	kcUser.EmailVerified = gocloak.BoolP(user.IsEmailVerified())
	kcUser.Enabled = gocloak.BoolP(user.IsEnabled())

	err = me.core.GetClient().UpdateUser(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		*kcUser,
	)
	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	return nil
}

func (me *admin) authenticate(ctx context.Context) error {
	if me.session != nil && me.session.IsValid(ctx) {
		return nil
	}

	token, err := me.core.GetClient().LoginAdmin(ctx, me.config.username, me.config.password, me.config.masterRealm)
	if err != nil {
		return err
	}
	_, claims, err := me.core.GetClient().DecodeAccessToken(ctx, token.AccessToken, me.config.masterRealm)
	if err != nil {
		return err
	}

	if sid, ok := (*claims)["sid"].(string); ok {
		defer func() {
			err := me.core.GetClient().LogoutUserSession(ctx, token.AccessToken, me.config.masterRealm, sid)
			if err != nil {
				qlog.Warn("Failed to logout user session: %v", err)
			}
		}()
	} else {
		qlog.Warn("No session id found in token claims")
	}

	client, err := me.getOrCreateClient(ctx, me.config.clientID, token.AccessToken, me.config.masterRealm)
	if err != nil {
		return err
	}

	me.session = client.GetSession(ctx)
	return nil
}

func (me *admin) getOrCreateClient(ctx context.Context, clientID, accessToken, realm string) (Client, error) {
	// Try to find existing client
	clients, err := me.core.GetClient().GetClients(
		ctx,
		accessToken,
		realm,
		gocloak.GetClientsParams{
			ClientID: &clientID,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get clients: %w", err)
	}

	var client *gocloak.Client
	var secret string

	if len(clients) > 0 {
		// Client exists, get the client secret
		client = clients[0]
		secretObj, err := me.core.GetClient().GetClientSecret(
			ctx,
			accessToken,
			realm,
			*client.ID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get client secret: %w", err)
		}
		secret = *secretObj.Value
	} else {
		// Create new client
		newClient := gocloak.Client{
			ClientID:                  &clientID,
			Enabled:                   gocloak.BoolP(true),
			StandardFlowEnabled:       gocloak.BoolP(true),
			DirectAccessGrantsEnabled: gocloak.BoolP(true),
			ServiceAccountsEnabled:    gocloak.BoolP(true),
		}

		clientID, err := me.core.GetClient().CreateClient(
			ctx,
			accessToken,
			realm,
			newClient,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create client: %w", err)
		}

		// Get newly created client
		newClientObj, err := me.core.GetClient().GetClient(
			ctx,
			accessToken,
			realm,
			clientID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get created client: %w", err)
		}
		client = newClientObj

		// Generate client secret if not service account
		secretObj, err := me.core.GetClient().GetClientSecret(
			ctx,
			accessToken,
			realm,
			clientID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get client secret: %w", err)
		}
		secret = *secretObj.Value
	}

	return NewClient(me.core, *client.ClientID, secret, realm), nil
}

func (me *admin) Session(ctx context.Context) Session {
	if me.authenticate(ctx) != nil {
		qlog.Error("Failed to authenticate admin")
		return NewSession(me.core, nil, "", "", "")
	}

	return me.session
}
