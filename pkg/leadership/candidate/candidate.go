package candidate

import (
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

	store data.Store

	availabilityCriteria []leadership.AvailabilityCriteria

	currentState leadership.State

	applicationName       string
	applicationInstanceId string

	keygen KeyGenerator

	candidateUpdateTicker *time.Ticker
	leaderAttemptTicker   *time.Ticker
	leaseRenewalTicker    *time.Ticker
}

func New(store data.Store) leadership.Candidate {
	c := &Candidate{
		store:                 store,
		candidateUpdateTicker: time.NewTicker(LeaseTimeout / 2),
		leaderAttemptTicker:   time.NewTicker(LeaseTimeout),
		leaseRenewalTicker:    time.NewTicker(LeaseTimeout / 2),
		becameLeader:          signal.NewSignal(),
		losingLeadership:      signal.NewSignal(),
		becameFollower:        signal.NewSignal(),
		becameUnavailable:     signal.NewSignal(),
	}

	return c
}

func (c *Candidate) SetState(newState leadership.State) {
	if c.currentState != nil && c.currentState.Name() == newState.Name() {
		return
	}

	previousState := c.currentState
	c.currentState = newState
	c.currentState.OnEnterState(c, previousState)
}

func (c *Candidate) IsAvailable() bool {
	for _, criteria := range c.availabilityCriteria {
		if !criteria() {
			return false
		}
	}

	return true
}

func (c *Candidate) IsCurrentLeader() bool {
	return c.store.TempGet(c.keygen.GetLeaderKey(c.applicationName)) == c.applicationInstanceId
}

func (c *Candidate) UpdateCandidateStatus(available bool) {
	if available {
		c.store.SortedSetAdd(c.keygen.GetLeaderCandidatesKey(c.applicationName), c.applicationInstanceId, float64(time.Now().UnixNano()))
	} else {
		c.store.SortedSetRemove(c.keygen.GetLeaderCandidatesKey(c.applicationName), c.applicationInstanceId)
	}
}

func (c *Candidate) TrimCandidates() {
	// Remove all entries except the newest MaxCandidates entries
	// Note: 0 is lowest score (oldest), -1 is highest score (newest)
	c.store.SortedSetRemoveRangeByRank(c.keygen.GetLeaderCandidatesKey(c.applicationName), 0, -(MaxCandidates + 1))
}

func (c *Candidate) GetLeaderCandidates() []string {
	now := float64(time.Now().UnixNano())
	min := fmt.Sprintf("%f", now-float64(LeaseTimeout.Nanoseconds()))
	max := fmt.Sprintf("%f", now)

	members := c.store.SortedSetRangeByScoreWithScores(
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

func (c *Candidate) TryBecomeLeader() bool {
	return c.store.TempSet(c.keygen.GetLeaderKey(c.applicationName), c.applicationInstanceId, LeaseTimeout)
}

func (c *Candidate) RenewLeadershipLease() {
	c.store.TempExpire(c.keygen.GetLeaderKey(c.applicationName), LeaseTimeout)
}

func (c *Candidate) AddAvailabilityCriteria(criteria leadership.AvailabilityCriteria) {
	c.availabilityCriteria = append(c.availabilityCriteria, criteria)
}

func (c *Candidate) Init() {
	c.applicationName = app.GetApplicationName()
	c.applicationInstanceId = app.GetApplicationInstanceId()

	log.Info("[Candidate::Init] Application instance ID: %s", c.applicationInstanceId)

	c.SetState(states.NewUnavailable())
}

func (c *Candidate) Deinit() {
	c.SetState(states.NewUnavailable())

	c.candidateUpdateTicker.Stop()
	c.leaderAttemptTicker.Stop()
	c.leaseRenewalTicker.Stop()
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

func (c *Candidate) onBecameLeader() {
	log.Info("[Candidate::onBecameLeader] Became the leader (instanceId=%s)", c.applicationInstanceId)
	c.UpdateCandidateStatus(true)
}

func (c *Candidate) onBecameFollower() {
	log.Info("[Candidate::onBecameFollower] Became a follower (instanceId=%s)", c.applicationInstanceId)
	c.UpdateCandidateStatus(true)
}

func (c *Candidate) onBecameUnavailable() {
	log.Info("[Candidate::onBecameUnavailable] Became unavailable (instanceId=%s)", c.applicationInstanceId)
	c.UpdateCandidateStatus(false)
}

func (c *Candidate) SetLeaderAndCandidateFields() {
	services := query.New(c.store).
		ForType("Service").
		Where("ApplicationName").Equals(c.applicationName).
		Execute()

	candidates := c.GetLeaderCandidates()

	for _, service := range services {
		service.GetField("Leader").WriteString(c.applicationInstanceId, data.WriteChanges)
		service.GetField("Candidates").WriteString(strings.Join(candidates, ","), data.WriteChanges)
		service.GetField("HeartbeatTrigger").WriteInt(0)
	}
}

func (c *Candidate) ClearLeaderAndCandidateFields() {
	services := query.New(c.store).
		ForType("Service").
		Where("ApplicationName").Equals(c.applicationName).
		Execute()

	candidates := c.GetLeaderCandidates()

	for _, service := range services {
		leaderField := service.GetField("Leader")
		if leaderField.ReadString() == c.applicationInstanceId {
			leaderField.WriteString("")
		}

		candidatesField := service.GetField("Candidates")
		if candidatesField.ReadString() != "" {
			candidatesField.WriteString(strings.Join(candidates, ","))
		}
	}
}

func (c *Candidate) onLosingLeadership() {
	log.Info("[Candidate::onLosingLeadership] Losing leadership status (instanceId=%s)", c.applicationInstanceId)

	c.UpdateCandidateStatus(false)
	c.ClearLeaderAndCandidateFields()
}

func (c *Candidate) DoWork() {
	c.currentState.DoWork(c)
}
