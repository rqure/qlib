package states

type Enum int

const (
	UnavailableState Enum = iota
	FollowerState
	LeaderState
)

func (s Enum) String() string {
	switch s {
	case UnavailableState:
		return "Unavailable"
	case FollowerState:
		return "Follower"
	case LeaderState:
		return "Leader"
	default:
		return "Unknown"
	}
}
