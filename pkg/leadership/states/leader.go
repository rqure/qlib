package states

import (
	"context"

	"github.com/rqure/qlib/pkg/leadership"
)

type Leader struct {
	Base
}

func NewLeader() leadership.State {
	return &Leader{Base{LeaderState}}
}

func (s *Leader) DoWork(ctx context.Context, c leadership.Candidate) {
	if !c.IsAvailable() {
		c.SetState(ctx, NewUnavailable())
		return
	}

	for {
		select {
		case <-c.IsCurrentLeaderCheck():
			if !c.IsCurrentLeader(ctx) {
				c.SetState(ctx, NewFollower())
				return
			}
		case <-c.LeaseRenewal():
			c.RenewLeadershipLease(ctx)
		case <-c.CandidateUpdate():
			c.UpdateCandidateStatus(ctx, true)
			c.TrimCandidates(ctx)
			c.SetLeaderAndCandidateFields(ctx)
		default:
			return
		}
	}
}

func (s *Leader) OnEnterState(ctx context.Context, c leadership.Candidate, previousState leadership.State) {
	c.BecameLeader().Emit(ctx)
}
