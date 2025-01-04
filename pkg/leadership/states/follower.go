package states

import (
	"context"

	"github.com/rqure/qlib/pkg/leadership"
)

type Follower struct {
	Base
}

func NewFollower() leadership.State {
	return &Follower{Base{FollowerState}}
}

func (s *Follower) DoWork(ctx context.Context, c leadership.Candidate) {
	if !c.IsAvailable() {
		c.SetState(ctx, NewUnavailable())
		return
	}

	for {
		select {
		case <-c.IsCurrentLeaderCheck():
			if c.IsCurrentLeader(ctx) {
				c.SetState(ctx, NewLeader())
				return
			}
		case <-c.LeaderAttempt():
			if c.TryBecomeLeader(ctx) {
				c.SetState(ctx, NewLeader())
				return
			}
		case <-c.CandidateUpdate():
			c.UpdateCandidateStatus(ctx, true)
		default:
			return
		}
	}
}

func (s *Follower) OnEnterState(ctx context.Context, c leadership.Candidate, previousState leadership.State) {
	wasLeader := previousState != nil && previousState.Name() == LeaderState.String()
	if wasLeader {
		c.LosingLeadership().Emit(ctx)
	}

	c.BecameFollower().Emit(ctx)

	if c.TryBecomeLeader(ctx) {
		c.SetState(ctx, NewLeader())
	}
}
