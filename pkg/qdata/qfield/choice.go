package qfield

import (
	"github.com/rqure/qlib/pkg/qdata"
)

type ChoiceImpl struct {
	selectedIndex int64
	options       []string
}

func NewChoice(index int64) qdata.Choice {
	return &ChoiceImpl{
		selectedIndex: index,
	}
}

func NewCompleteChoice(index int64, options []string) qdata.CompleteChoice {
	return &ChoiceImpl{
		selectedIndex: index,
		options:       options,
	}
}

func (c *ChoiceImpl) Index() int64 {
	return c.selectedIndex
}

func (c *ChoiceImpl) Option() string {
	if c.IsValid() {
		return c.options[c.selectedIndex]
	}
	return ""
}

func (c *ChoiceImpl) Options() []string {
	result := make([]string, len(c.options))
	copy(result, c.options)
	return result
}

func (c *ChoiceImpl) SetIndex(index int64) qdata.Choice {
	c.selectedIndex = index
	return c
}

func (c *ChoiceImpl) SetOptions(options []string) qdata.Choice {
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
