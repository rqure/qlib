package auth

type UserDetails struct {
	ID            string
	Username      string
	Email         string
	FirstName     string
	LastName      string
	Enabled       bool
	Roles         []string
	EmailVerified bool
}

type User interface {
	GetDetails() *UserDetails
	GetID() string
	GetUsername() string
	GetEmail() string
	IsEnabled() bool
	GetRoles() []string
}

type user struct {
	details *UserDetails
}

func NewUser(info *UserInfo) User {
	return &user{
		details: &UserDetails{
			ID:            info.ID,
			Username:      info.Username,
			Email:         info.Email,
			FirstName:     info.FirstName,
			LastName:      info.LastName,
			Enabled:       info.Enabled,
			Roles:         info.Roles,
			EmailVerified: info.EmailVerified,
		},
	}
}

func (u *user) GetDetails() *UserDetails {
	return u.details
}

func (u *user) GetID() string {
	return u.details.ID
}

func (u *user) GetUsername() string {
	return u.details.Username
}

func (u *user) GetEmail() string {
	return u.details.Email
}

func (u *user) IsEnabled() bool {
	return u.details.Enabled
}

func (u *user) GetRoles() []string {
	return u.details.Roles
}
