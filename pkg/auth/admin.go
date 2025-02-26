package auth

import (
	"context"
	"fmt"

	"github.com/Nerzal/gocloak/v13"
)

type Admin interface {
	EnsureSetup(ctx context.Context) error
	GetOrCreateClient(ctx context.Context, clientID string) (Client, error)

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
	realm    string
	username string
	password string
}

type AdminOption func(*adminConfig)

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

func NewAdmin(core Core, opts ...AdminOption) Admin {
	config := &adminConfig{}

	defaultOpts := []AdminOption{
		Realm(getEnvOrDefault("Q_KEYCLOAK_REALM", "qcore-realm")),
		Username(getEnvOrDefault("Q_KEYCLOAK_ADMIN_USER", "admin")),
		Password(getEnvOrDefault("Q_KEYCLOAK_ADMIN_PASSWORD", "admin")),
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

	// Try to find existing client
	clients, err := me.core.GetClient().GetClients(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
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
			me.session.AccessToken(),
			me.config.realm,
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
			me.session.AccessToken(),
			me.config.realm,
			newClient,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create client: %w", err)
		}

		// Get newly created client
		newClientObj, err := me.core.GetClient().GetClient(
			ctx,
			me.session.AccessToken(),
			me.config.realm,
			clientID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get created client: %w", err)
		}
		client = newClientObj

		// Generate client secret if not service account
		secretObj, err := me.core.GetClient().GetClientSecret(
			ctx,
			me.session.AccessToken(),
			me.config.realm,
			clientID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to get client secret: %w", err)
		}
		secret = *secretObj.Value
	}

	return NewClient(me.core, *client.ClientID, secret), nil
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
	roles, err := me.getUserRoles(ctx, kcUser)
	if err != nil {
		return nil, err
	}

	// Convert to our User type using proper setters
	user := NewUser()
	// Set ID (assuming there's a SetID method or we need to add it)
	// For now we'll skip setting ID as it's not in the interface
	user.SetUsername(username)
	if kcUser.FirstName != nil {
		user.SetFirstName(*kcUser.FirstName)
	}
	if kcUser.LastName != nil {
		user.SetLastName(*kcUser.LastName)
	}
	if kcUser.Email != nil {
		user.SetEmail(*kcUser.Email)
	}
	if kcUser.EmailVerified != nil {
		user.SetEmailVerified(*kcUser.EmailVerified)
	}
	if kcUser.Enabled != nil {
		user.SetEnabled(*kcUser.Enabled)
	}
	user.SetRoles(roles)

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
		gocloak.GetUsersParams{},
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

		// Get user roles
		roles, err := me.getUserRoles(ctx, kcUser)
		if err != nil {
			return nil, err
		}

		user := NewUser()
		// Set values using proper setters
		user.SetUsername(*kcUser.Username)
		if kcUser.FirstName != nil {
			user.SetFirstName(*kcUser.FirstName)
		}
		if kcUser.LastName != nil {
			user.SetLastName(*kcUser.LastName)
		}
		if kcUser.Email != nil {
			user.SetEmail(*kcUser.Email)
		}
		if kcUser.EmailVerified != nil {
			user.SetEmailVerified(*kcUser.EmailVerified)
		}
		if kcUser.Enabled != nil {
			user.SetEnabled(*kcUser.Enabled)
		}
		user.SetRoles(roles)

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

	// Update user roles (this would require additional implementation
	// to handle role assignment/removal)
	// TODO: Implement role management if needed

	return nil
}

// getUserRoles retrieves all roles for a user
func (me *admin) getUserRoles(ctx context.Context, kcUser *gocloak.User) ([]string, error) {
	realmRoles, err := me.core.GetClient().GetRealmRolesByUserID(
		ctx,
		me.session.AccessToken(),
		me.config.realm,
		*kcUser.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get user roles: %w", err)
	}

	roles := make([]string, 0, len(realmRoles))
	for _, role := range realmRoles {
		if role.Name != nil {
			roles = append(roles, *role.Name)
		}
	}

	return roles, nil
}

func (me *admin) authenticate(ctx context.Context) error {
	if me.session != nil && !me.session.IsExpired() {
		return nil
	}

	token, err := me.core.GetClient().LoginAdmin(ctx, me.config.username, me.config.password, me.config.realm)
	if err != nil {
		return err
	}

	me.session = NewSession(me.core, token)
	return nil
}
