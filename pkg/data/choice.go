package data

type Choice interface {
	// selected index
	Index() int64

	// selected option
	Option() string

	// all options
	Options() []string

	SetIndex(index int64) Choice
	SetOptions(options []string) Choice
	IsValid() bool
	Count() int
	Select(option string) bool
}
