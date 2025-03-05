package auth

import "github.com/Nerzal/gocloak/v13"

type User interface {
	GetID() string
	GetUsername() string
	GetFirstName() string
	GetLastName() string
	GetEmail() string
	IsEmailVerified() bool
	IsEnabled() bool
	JSON() string
}

type user struct {
	model *gocloak.User
}

func NewUser(u *gocloak.User) User {
	return &user{
		model: u,
	}
}

func (u *user) GetID() string {
	return *u.model.ID
}

func (u *user) GetUsername() string {
	return *u.model.Username
}

func (u *user) GetFirstName() string {
	return *u.model.FirstName
}

func (u *user) GetLastName() string {
	return *u.model.LastName
}

func (u *user) GetEmail() string {
	return *u.model.Email
}

func (u *user) IsEmailVerified() bool {
	return *u.model.EmailVerified
}

func (u *user) IsEnabled() bool {
	return *u.model.Enabled
}

func (u *user) JSON() string {
	return u.model.String()
}
