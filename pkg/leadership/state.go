package leadership

import "context"

type State interface {
	DoWork(context.Context, Candidate)
	OnEnterState(context.Context, Candidate, State)
	Name() string
}
