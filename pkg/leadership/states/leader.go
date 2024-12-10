package states

import "github.com/rqure/qlib/pkg/leadership"

type Leader struct {
	Base
}

func NewLeader() leadership.State {
	return &Leader{Base{LeaderState}}
}

func (s *Leader) DoWork(c leadership.Candidate) {
	if !c.IsAvailable() {
		c.SetState(NewUnavailable())
		return
	}

	if !c.IsCurrentLeader() {
		c.SetState(NewFollower())
		return
	}

	for {
		select {
		case <-c.LeaseRenewal():
			c.RenewLeadershipLease()
		case <-c.CandidateUpdate():
			c.UpdateCandidateStatus(true)
			c.TrimCandidates()
			c.SetLeaderAndCandidateFields()
		default:
			return
		}
	}
}

func (s *Leader) OnEnterState(c leadership.Candidate, previousState leadership.State) {
	c.BecameLeader().Emit()
}
