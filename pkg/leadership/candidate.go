package leadership

import (
	"time"

	"github.com/rqure/qlib/pkg/signalslots"
)

type AvailabilityCriteria func() bool

type Candidate interface {
	Init()
	Deinit()
	AddAvailabilityCriteria(AvailabilityCriteria)
	TryBecomeLeader() bool
	RenewLeadershipLease()
	IsAvailable() bool
	IsCurrentLeader() bool
	UpdateCandidateStatus(bool)
	TrimCandidates()
	GetLeaderCandidates() []string
	SetState(State)
	SetLeaderAndCandidateFields()
	DoWork()

	LosingLeadership() signalslots.Signal
	BecameLeader() signalslots.Signal
	BecameFollower() signalslots.Signal
	BecameUnavailable() signalslots.Signal

	CandidateUpdate() <-chan time.Time
	LeaderAttempt() <-chan time.Time
	LeaseRenewal() <-chan time.Time
}
