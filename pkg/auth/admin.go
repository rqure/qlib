package auth

import (
	"context"
	"fmt"
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

	// Initialize realm
	if err := me.ensureRealmExists(); err != nil {
		return fmt.Errorf("realm setup failed: %w", err)
	}

	// Initialize default roles
	if err := me.ensureRolesExist(); err != nil {
		return fmt.Errorf("roles setup failed: %w", err)
	}

	return nil
}

// GetOrCreateClient creates a new client or gets an existing one
func (me *admin) GetOrCreateClient(ctx context.Context, clientID string) (Client, error) {
	if err := me.authenticate(ctx); err != nil {
		return nil, err
	}

	// Get or create client
	secret, err := me.createOrGetClientSecret(clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create client: %w", err)
	}

	// Create client instance
	return NewClient(me.core, clientID, secret), nil
}

// CreateUser creates a new user with the given username and password
func (me *admin) CreateUser(ctx context.Context, username, password string) error {
	if err := me.authenticate(ctx); err != nil {
		return err
	}

	// Create user with minimal information
	_, err := me.createUser(username, username+"@example.com", username, "", password)
	return err
}

// DeleteUser deletes the user with the given username
func (me *admin) DeleteUser(ctx context.Context, username string) error {
	if err := me.authenticate(ctx); err != nil {
		return err
	}

	// Find user ID by username
	userInfo, err := me.getUserByUsername(username)
	if err != nil {
		return err
	}

	return me.deleteUser(userInfo.ID)
}

// GetUser retrieves a user by username
func (me *admin) GetUser(ctx context.Context, username string) (User, error) {
	if err := me.authenticate(ctx); err != nil {
		return nil, err
	}

	userInfo, err := me.getUserByUsername(username)
	if err != nil {
		return nil, err
	}

	return NewUser(userInfo), nil
}

// GetUsers retrieves all users
func (me *admin) GetUsers(ctx context.Context) (map[string]User, error) {
	if err := me.authenticate(ctx); err != nil {
		return nil, err
	}

	usersInfo, err := me.getUsers()
	if err != nil {
		return nil, err
	}

	users := make(map[string]User)
	for _, userInfo := range usersInfo {
		users[userInfo.Username] = NewUser(userInfo)
	}

	return users, nil
}

// UpdateUser updates a user's information
func (me *admin) UpdateUser(ctx context.Context, user User) error {
	if err := me.authenticate(ctx); err != nil {
		return err
	}

	// Extract user details from the User interface
	details := user.GetDetails()
	if details == nil {
		return fmt.Errorf("no user details available")
	}

	// Find user ID by username
	userInfo, err := me.getUserByUsername(details.Username)
	if err != nil {
		return err
	}

	// Prepare updates
	updates := map[string]interface{}{
		"email":     details.Email,
		"firstName": details.FirstName,
		"lastName":  details.LastName,
		"enabled":   details.Enabled,
	}

	return me.updateUser(userInfo.ID, updates)
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

func (me *admin) ensureRealmExists() error {
	// Implementation using a.core.GetClient()
	return nil
}

func (me *admin) ensureRolesExist() error {
	// Implementation using a.core.GetClient()
	return nil
}

func (me *admin) createOrGetClientSecret(clientID string) (string, error) {
	// Implementation using a.core.GetClient()
	return "client-secret", nil
}

func (me *admin) createUser(username, email, firstName, lastName, password string) (string, error) {
	// Implementation using a.core.GetClient()
	return "user-id", nil
}

func (me *admin) deleteUser(userID string) error {
	// Implementation using a.core.GetClient()
	return nil
}

func (me *admin) getUserByUsername(username string) (*UserInfo, error) {
	// Implementation using a.core.GetClient()
	return &UserInfo{
		ID:       "user-id",
		Username: username,
		Email:    username + "@example.com",
		Enabled:  true,
	}, nil
}

func (me *admin) getUsers() ([]*UserInfo, error) {
	// Implementation using a.core.GetClient()
	return []*UserInfo{}, nil
}

func (me *admin) updateUser(userID string, updates map[string]interface{}) error {
	// Implementation using a.core.GetClient()
	return nil
}
