package states

type Base struct {
	name Enum
}

func (s *Base) Name() string {
	return s.name.String()
}
