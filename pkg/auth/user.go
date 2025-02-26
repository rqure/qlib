package auth

type User interface {
	GetID() string
	GetUsername() string
	GetFirstName() string
	GetLastName() string
	GetEmail() string
	IsEmailVerified() bool
	IsEnabled() bool
	GetRoles() []string

	SetUsername(username string)
	SetFirstName(firstName string)
	SetLastName(lastName string)
	SetEmail(email string)
	SetEmailVerified(emailVerified bool)
	SetEnabled(enabled bool)
	SetRoles(roles []string)
}

type user struct {
	ID            string
	Username      string
	Email         string
	FirstName     string
	LastName      string
	Enabled       bool
	Roles         []string
	EmailVerified bool
}

func NewUser() User {
	return &user{}
}

func (u *user) GetID() string {
	return u.ID
}

func (u *user) GetUsername() string {
	return u.Username
}

func (u *user) GetFirstName() string {
	return u.FirstName
}

func (u *user) GetLastName() string {
	return u.LastName
}

func (u *user) GetEmail() string {
	return u.Email
}

func (u *user) IsEmailVerified() bool {
	return u.EmailVerified
}

func (u *user) IsEnabled() bool {
	return u.Enabled
}

func (u *user) GetRoles() []string {
	return u.Roles
}

func (u *user) SetUsername(username string) {
	u.Username = username
}

func (u *user) SetFirstName(firstName string) {
	u.FirstName = firstName
}

func (u *user) SetLastName(lastName string) {
	u.LastName = lastName
}

func (u *user) SetEmail(email string) {
	u.Email = email
}

func (u *user) SetEmailVerified(emailVerified bool) {
	u.EmailVerified = emailVerified
}

func (u *user) SetEnabled(enabled bool) {
	u.Enabled = enabled
}

func (u *user) SetRoles(roles []string) {
	u.Roles = roles
}
