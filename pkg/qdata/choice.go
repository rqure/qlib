package qdata

type Choice interface {
	// selected index
	Index() int64
	SetIndex(index int64) Choice
}

type CompleteChoice interface {
	Choice

	// selected option
	Option() string

	// all options
	Options() []string

	SetOptions(options []string) Choice

	IsValid() bool
	Count() int
	Select(option string) bool
}
