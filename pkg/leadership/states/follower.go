package states

import "github.com/rqure/qlib/pkg/leadership"

type Follower struct {
	Base
}

func NewFollower() leadership.State {
	return &Follower{Base{FollowerState}}
}

func (s *Follower) DoWork(c leadership.Candidate) {
	if !c.IsAvailable() {
		c.SetState(NewUnavailable())
		return
	}

	if c.IsCurrentLeader() {
		c.SetState(NewLeader())
		return
	}

	for {
		select {
		case <-c.LeaderAttempt():
			if c.TryBecomeLeader() {
				c.SetState(NewLeader())
				return
			}
		case <-c.CandidateUpdate():
			c.UpdateCandidateStatus(true)
		default:
			return
		}
	}
}

func (s *Follower) OnEnterState(c leadership.Candidate, previousState leadership.State) {
	wasLeader := previousState != nil && previousState.Name() == LeaderState.String()
	if wasLeader {
		c.LosingLeadership().Emit(nil)
	}

	c.BecameFollower().Emit(nil)

	if c.TryBecomeLeader() {
		c.SetState(NewLeader())
	}
}
