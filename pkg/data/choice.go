package data

// Choice represents a selected option from a list of choices
type Choice interface {
	// GetSelectedIndex returns the index of the currently selected choice
	GetSelectedIndex() int64

	// GetSelectedValue returns the value of the currently selected choice,
	// or empty string if index is out of bounds
	GetSelectedValue() string

	// GetOptions returns all available choice options
	GetOptions() []string

	// SetSelectedIndex updates the selected index
	SetSelectedIndex(index int64) Choice

	// SetOptions updates the available options
	SetOptions(options []string) Choice

	// IsValid returns true if the selected index is valid within the options
	IsValid() bool

	// Count returns the number of options
	Count() int

	// Select sets the selected index by option value rather than index
	// Returns true if found and selected, false if not found
	Select(option string) bool
}
