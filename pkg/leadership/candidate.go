package leadership

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/signalslots"
)

type AvailabilityCriteria func() bool

type Candidate interface {
	Init(context.Context)
	Deinit(context.Context)
	AddAvailabilityCriteria(AvailabilityCriteria)
	TryBecomeLeader(context.Context) bool
	RenewLeadershipLease(context.Context)
	IsAvailable() bool
	IsCurrentLeader(context.Context) bool
	UpdateCandidateStatus(context.Context, bool)
	TrimCandidates(context.Context)
	GetLeaderCandidates(context.Context) []string
	SetState(context.Context, State)
	SetLeaderAndCandidateFields(context.Context)
	DoWork(context.Context)

	LosingLeadership() signalslots.Signal
	BecameLeader() signalslots.Signal
	BecameFollower() signalslots.Signal
	BecameUnavailable() signalslots.Signal

	IsCurrentLeaderCheck() <-chan time.Time
	CandidateUpdate() <-chan time.Time
	LeaderAttempt() <-chan time.Time
	LeaseRenewal() <-chan time.Time
}
