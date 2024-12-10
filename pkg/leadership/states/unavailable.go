package states

import "github.com/rqure/qlib/pkg/leadership"

// Concrete state implementations
type Unavailable struct {
	Base
}

func NewUnavailable() leadership.State {
	return &Unavailable{Base{UnavailableState}}
}

func (s *Unavailable) DoWork(c leadership.Candidate) {
	if c.IsAvailable() {
		c.SetState(NewFollower())
		return
	}

	for {
		select {
		case <-c.CandidateUpdate():
			c.UpdateCandidateStatus(false)
		default:
			return
		}
	}
}

func (s *Unavailable) OnEnterState(c leadership.Candidate, previousState leadership.State) {
	wasLeader := previousState != nil && previousState.Name() == LeaderState.String()
	if wasLeader {
		c.LosingLeadership().Emit()
	}

	c.BecameUnavailable().Emit()
}
