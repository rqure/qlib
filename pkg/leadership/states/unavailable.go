package states

import (
	"context"

	"github.com/rqure/qlib/pkg/leadership"
)

// Concrete state implementations
type Unavailable struct {
	Base
}

func NewUnavailable() leadership.State {
	return &Unavailable{Base{UnavailableState}}
}

func (s *Unavailable) DoWork(ctx context.Context, c leadership.Candidate) {
	if c.IsAvailable() {
		c.SetState(ctx, NewFollower())
		return
	}

	for {
		select {
		case <-c.CandidateUpdate():
			c.UpdateCandidateStatus(ctx, false)
		default:
			return
		}
	}
}

func (s *Unavailable) OnEnterState(ctx context.Context, c leadership.Candidate, previousState leadership.State) {
	wasLeader := previousState != nil && previousState.Name() == LeaderState.String()
	if wasLeader {
		c.LosingLeadership().Emit()
	}

	c.BecameUnavailable().Emit()
}
