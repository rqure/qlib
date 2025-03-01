package field

import (
	"github.com/rqure/qlib/pkg/data"
)

// ChoiceImpl implements the Choice interface
type ChoiceImpl struct {
	selectedIndex int64
	options       []string
}

// NewChoice creates a new Choice with the given selected index and options
func NewChoice(index int64, options []string) data.Choice {
	return &ChoiceImpl{
		selectedIndex: index,
		options:       options,
	}
}

// LoadOptions loads options from schema
func (c *ChoiceImpl) LoadOptions(options []string) {
	c.options = options
}

func (c *ChoiceImpl) GetSelectedIndex() int64 {
	return c.selectedIndex
}

func (c *ChoiceImpl) GetSelectedValue() string {
	if c.IsValid() {
		return c.options[c.selectedIndex]
	}
	return ""
}

func (c *ChoiceImpl) GetOptions() []string {
	return c.options
}

func (c *ChoiceImpl) SetSelectedIndex(index int64) data.Choice {
	c.selectedIndex = index
	return c
}

func (c *ChoiceImpl) SetOptions(options []string) data.Choice {
	c.options = options
	return c
}

func (c *ChoiceImpl) IsValid() bool {
	return c.selectedIndex >= 0 && int(c.selectedIndex) < len(c.options)
}

func (c *ChoiceImpl) Count() int {
	return len(c.options)
}

func (c *ChoiceImpl) Select(option string) bool {
	for i, opt := range c.options {
		if opt == option {
			c.selectedIndex = int64(i)
			return true
		}
	}
	return false
}
