package candidate

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/leadership"
	"github.com/rqure/qlib/pkg/leadership/states"
	"github.com/rqure/qlib/pkg/log"
	"github.com/rqure/qlib/pkg/signalslots"
	"github.com/rqure/qlib/pkg/signalslots/signal"
)

const LeaseTimeout = 15 * time.Second
const MaxCandidates = 5

type Candidate struct {
	becameLeader      signalslots.Signal
	losingLeadership  signalslots.Signal
	becameFollower    signalslots.Signal
	becameUnavailable signalslots.Signal

	leaderStore leadership.Store
	dataStore   data.Store

	availabilityCriteria []leadership.AvailabilityCriteria

	currentState leadership.State

	applicationName       string
	applicationInstanceId string

	keygen KeyGenerator

	IsCurrentLeaderTicker *time.Ticker
	candidateUpdateTicker *time.Ticker
	leaderAttemptTicker   *time.Ticker
	leaseRenewalTicker    *time.Ticker
}

func New(ls leadership.Store, ds data.Store) leadership.Candidate {
	c := &Candidate{
		leaderStore:           ls,
		dataStore:             ds,
		candidateUpdateTicker: time.NewTicker(LeaseTimeout / 2),
		leaderAttemptTicker:   time.NewTicker(LeaseTimeout),
		leaseRenewalTicker:    time.NewTicker(LeaseTimeout / 2),
		IsCurrentLeaderTicker: time.NewTicker(LeaseTimeout / 5),
		becameLeader:          signal.New(),
		losingLeadership:      signal.New(),
		becameFollower:        signal.New(),
		becameUnavailable:     signal.New(),
		currentState:          states.NewUnavailable(),
	}

	c.becameFollower.Connect(c.onBecameFollower)
	c.becameLeader.Connect(c.onBecameLeader)
	c.becameUnavailable.Connect(c.onBecameUnavailable)
	c.losingLeadership.Connect(c.onLosingLeadership)

	return c
}

func (c *Candidate) SetState(ctx context.Context, newState leadership.State) {
	if c.currentState != nil && c.currentState.Name() == newState.Name() {
		return
	}

	previousState := c.currentState
	c.currentState = newState
	c.currentState.OnEnterState(ctx, c, previousState)
}

func (c *Candidate) IsAvailable() bool {
	for _, criteria := range c.availabilityCriteria {
		if !criteria() {
			return false
		}
	}

	return true
}

func (c *Candidate) IsCurrentLeader(ctx context.Context) bool {
	currentLeaderId := c.leaderStore.TempGet(ctx, c.keygen.GetLeaderKey(c.applicationName))
	log.Debug("applicationInstanceId=(%s); currentLeaderId=(%s)", c.applicationInstanceId, currentLeaderId)
	return currentLeaderId == c.applicationInstanceId
}

func (c *Candidate) UpdateCandidateStatus(ctx context.Context, available bool) {
	if available {
		c.leaderStore.SortedSetAdd(ctx, c.keygen.GetLeaderCandidatesKey(c.applicationName), c.applicationInstanceId, float64(time.Now().UnixNano()))
	} else {
		c.leaderStore.SortedSetRemove(ctx, c.keygen.GetLeaderCandidatesKey(c.applicationName), c.applicationInstanceId)
	}
}

func (c *Candidate) TrimCandidates(ctx context.Context) {
	// Remove all entries except the newest MaxCandidates entries
	// Note: 0 is lowest score (oldest), -1 is highest score (newest)
	c.leaderStore.SortedSetRemoveRangeByRank(ctx, c.keygen.GetLeaderCandidatesKey(c.applicationName), 0, -(MaxCandidates + 1))
}

func (c *Candidate) GetLeaderCandidates(ctx context.Context) []string {
	now := float64(time.Now().UnixNano())
	min := fmt.Sprintf("%f", now-float64(LeaseTimeout.Nanoseconds()))
	max := fmt.Sprintf("%f", now)

	members := c.leaderStore.SortedSetRangeByScoreWithScores(
		ctx,
		c.keygen.GetLeaderCandidatesKey(c.applicationName),
		min,
		max,
	)

	candidates := make([]string, len(members))
	for i, member := range members {
		candidates[i] = member.Member
	}

	return candidates
}

func (c *Candidate) TryBecomeLeader(ctx context.Context) bool {
	return c.leaderStore.TempSet(ctx, c.keygen.GetLeaderKey(c.applicationName), c.applicationInstanceId, LeaseTimeout)
}

func (c *Candidate) RenewLeadershipLease(ctx context.Context) {
	c.leaderStore.TempExpire(ctx, c.keygen.GetLeaderKey(c.applicationName), LeaseTimeout)
}

func (c *Candidate) AddAvailabilityCriteria(criteria leadership.AvailabilityCriteria) {
	c.availabilityCriteria = append(c.availabilityCriteria, criteria)
}

func (c *Candidate) Init(context.Context) {
	c.applicationName = app.GetName()
	c.applicationInstanceId = app.GetApplicationInstanceId()

	log.Info("Application instance ID: %s", c.applicationInstanceId)
}

func (c *Candidate) Deinit(ctx context.Context) {
	c.SetState(ctx, states.NewUnavailable())

	c.candidateUpdateTicker.Stop()
	c.leaderAttemptTicker.Stop()
	c.leaseRenewalTicker.Stop()
	c.IsCurrentLeaderTicker.Stop()
}

func (c *Candidate) BecameLeader() signalslots.Signal {
	return c.becameLeader
}

func (c *Candidate) LosingLeadership() signalslots.Signal {
	return c.losingLeadership
}

func (c *Candidate) BecameFollower() signalslots.Signal {
	return c.becameFollower
}

func (c *Candidate) BecameUnavailable() signalslots.Signal {
	return c.becameUnavailable
}

func (c *Candidate) CandidateUpdate() <-chan time.Time {
	return c.candidateUpdateTicker.C
}

func (c *Candidate) LeaderAttempt() <-chan time.Time {
	return c.leaderAttemptTicker.C
}

func (c *Candidate) LeaseRenewal() <-chan time.Time {
	return c.leaseRenewalTicker.C
}

func (c *Candidate) IsCurrentLeaderCheck() <-chan time.Time {
	return c.IsCurrentLeaderTicker.C
}

func (c *Candidate) resetCandidateTicker() {
	c.candidateUpdateTicker.Reset(LeaseTimeout / 2)
}

func (c *Candidate) onBecameLeader(ctx context.Context) {
	log.Info("Became the leader (instanceId=%s)", c.applicationInstanceId)
	c.UpdateCandidateStatus(ctx, true)
	c.resetCandidateTicker()
}

func (c *Candidate) onBecameFollower(ctx context.Context) {
	log.Info("Became a follower (instanceId=%s)", c.applicationInstanceId)
	c.UpdateCandidateStatus(ctx, true)
	c.resetCandidateTicker()
}

func (c *Candidate) onBecameUnavailable(ctx context.Context) {
	log.Info("Became unavailable (instanceId=%s)", c.applicationInstanceId)
	c.UpdateCandidateStatus(ctx, false)
	c.resetCandidateTicker()
}

func (c *Candidate) SetLeaderAndCandidateFields(ctx context.Context) {
	services := query.New(c.dataStore).
		Select().
		From("Service").
		Where("ApplicationName").Equals(c.applicationName).
		Execute(ctx)

	candidates := c.GetLeaderCandidates(ctx)

	for _, service := range services {
		service.DoMulti(ctx, func(s data.EntityBinding) {
			s.GetField("Leader").WriteString(ctx, c.applicationInstanceId, data.WriteChanges)
			s.GetField("Candidates").WriteString(ctx, strings.Join(candidates, ","), data.WriteChanges)
			s.GetField("HeartbeatTrigger").WriteInt(ctx, 0)
		})
	}
}

func (c *Candidate) ClearLeaderAndCandidateFields(ctx context.Context) {
	services := query.New(c.dataStore).
		Select("Leader", "Candidates").
		From("Service").
		Where("ApplicationName").Equals(c.applicationName).
		Execute(ctx)

	candidates := c.GetLeaderCandidates(ctx)

	for _, service := range services {
		service.DoMulti(ctx, func(s data.EntityBinding) {
			if s.GetField("Leader").GetString() == c.applicationInstanceId {
				s.GetField("Leader").WriteString(ctx, "")
			}

			if s.GetField("Candidates").GetString() != "" {
				s.GetField("Candidates").WriteString(ctx, strings.Join(candidates, ","))
			}
		})
	}
}

func (c *Candidate) onLosingLeadership(ctx context.Context) {
	log.Info("Losing leadership status (instanceId=%s)", c.applicationInstanceId)

	c.UpdateCandidateStatus(ctx, false)
	c.ClearLeaderAndCandidateFields(ctx)
}

func (c *Candidate) DoWork(ctx context.Context) {
	c.currentState.DoWork(ctx, c)
}
