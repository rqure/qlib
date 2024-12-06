package qapplication

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	ss "github.com/rqure/qlib/pkg/signals"
)

type LeaderStates int

const (
	Unavailable LeaderStates = iota
	Follower
	Leader
)

// leader:<appName>:current
// leader:<appName>:candidates
type LeaderElectionKeyGenerator struct{}

func (g *LeaderElectionKeyGenerator) GetLeaderKey(app string) string {
	return "leader:" + app + ":current"
}

func (g *LeaderElectionKeyGenerator) GetLeaderCandidatesKey(app string) string {
	return "leader:" + app + ":candidates"
}

const LeaderLeaseTimeout = 15 * time.Second
const MaxCandidates = 5

type LeaderAvailabilityCriteria func() bool

type LeaderElectionWorkerSignals struct {
	BecameLeader      ss.Signal // Is the current leader
	LosingLeadership  ss.Signal // Is the current leader, but is about to lose leadership status
	BecameFollower    ss.Signal // Is available to become a leader, but not elected as a leader
	BecameUnavailable ss.Signal // Is not available to become a leader
}

// ILeaderState interface defines the behavior for each state
type ILeaderState interface {
	DoWork(w *LeaderElectionWorker)
	OnEnterState(w *LeaderElectionWorker, previousState ILeaderState)
	Name() LeaderStates
}

// Base state implementation
type baseState struct {
	name LeaderStates
}

func (s *baseState) Name() LeaderStates {
	return s.name
}

// Concrete state implementations
type unavailableState struct {
	baseState
}

func newUnavailableState() *unavailableState {
	return &unavailableState{baseState{Unavailable}}
}

func (s *unavailableState) DoWork(w *LeaderElectionWorker) {
	if w.isAvailable() {
		w.setState(newFollowerState())
		return
	}

	for {
		select {
		case <-w.candidateUpdateTicker.C:
			w.updateCandidateStatus(false)
		default:
			return
		}
	}
}

func (s *unavailableState) OnEnterState(w *LeaderElectionWorker, previousState ILeaderState) {
	wasLeader := previousState != nil && previousState.Name() == Leader
	if wasLeader {
		w.Signals.LosingLeadership.Emit()
	}

	w.Signals.BecameUnavailable.Emit()
}

type followerState struct {
	baseState
}

func newFollowerState() *followerState {
	return &followerState{baseState{Follower}}
}

func (s *followerState) DoWork(w *LeaderElectionWorker) {
	if !w.isAvailable() {
		w.setState(newUnavailableState())
		return
	}

	if w.isCurrentLeader() {
		w.setState(newLeaderState())
		return
	}

	for {
		select {
		case <-w.leaderAttemptTicker.C:
			if w.tryBecomeLeader() {
				w.setState(newLeaderState())
				return
			}
		case <-w.candidateUpdateTicker.C:
			w.updateCandidateStatus(true)
		default:
			return
		}
	}
}

func (s *followerState) OnEnterState(w *LeaderElectionWorker, previousState ILeaderState) {
	wasLeader := previousState != nil && previousState.Name() == Leader
	if wasLeader {
		w.Signals.LosingLeadership.Emit()
	}

	w.Signals.BecameFollower.Emit()

	if w.tryBecomeLeader() {
		w.setState(newLeaderState())
	}
}

type leaderState struct {
	baseState
}

func newLeaderState() *leaderState {
	return &leaderState{baseState{Leader}}
}

func (s *leaderState) DoWork(w *LeaderElectionWorker) {
	if !w.isAvailable() {
		w.setState(newUnavailableState())
		return
	}

	if !w.isCurrentLeader() {
		w.setState(newFollowerState())
		return
	}

	for {
		select {
		case <-w.leaseRenewalTicker.C:
			w.renewLeadershipLease()
		case <-w.candidateUpdateTicker.C:
			w.updateCandidateStatus(true)
			w.trimCandidates()
			w.setLeaderAndCandidateFields()
		default:
			return
		}
	}
}

func (s *leaderState) OnEnterState(w *LeaderElectionWorker, previousState ILeaderState) {
	w.Signals.BecameLeader.Emit()
}

// Modify LeaderElectionWorker struct
type LeaderElectionWorker struct {
	Signals LeaderElectionWorkerSignals

	db                         IDatabase
	leaderAvailabilityCriteria []LeaderAvailabilityCriteria
	currentState               ILeaderState
	applicationName            string
	applicationInstanceId      string
	keygen                     LeaderElectionKeyGenerator
	isDatabaseConnected        bool
	candidateUpdateTicker      *time.Ticker
	leaderAttemptTicker        *time.Ticker
	leaseRenewalTicker         *time.Ticker
}

// Update initialization
func NewLeaderElectionWorker(db IDatabase) *LeaderElectionWorker {
	w := &LeaderElectionWorker{
		db:                         db,
		leaderAvailabilityCriteria: []LeaderAvailabilityCriteria{},
		keygen:                     LeaderElectionKeyGenerator{},
		currentState:               newUnavailableState(),
		candidateUpdateTicker:      time.NewTicker(LeaderLeaseTimeout / 2),
		leaderAttemptTicker:        time.NewTicker(LeaderLeaseTimeout),
		leaseRenewalTicker:         time.NewTicker(LeaderLeaseTimeout / 2),
	}

	w.Signals.BecameLeader.Connect(Slot(w.onBecameLeader))
	w.Signals.LosingLeadership.Connect(Slot(w.onLosingLeadership))
	w.Signals.BecameFollower.Connect(Slot(w.onBecameFollower))
	w.Signals.BecameUnavailable.Connect(Slot(w.onBecameUnavailable))

	return w
}

func (w *LeaderElectionWorker) AddAvailabilityCriteria(criteria LeaderAvailabilityCriteria) {
	w.leaderAvailabilityCriteria = append(w.leaderAvailabilityCriteria, criteria)
}

func (w *LeaderElectionWorker) Init() {
	w.applicationName = GetApplicationName()

	if os.Getenv("db_IN_DOCKER") != "" {
		w.applicationInstanceId = os.Getenv("HOSTNAME")
	}

	if w.applicationInstanceId == "" {
		w.applicationInstanceId = w.randomString()
	}

	Info("[LeaderElectionWorker::Init] Application instance ID: %s", w.applicationInstanceId)

	w.AddAvailabilityCriteria(func() bool {
		return w.isDatabaseConnected
	})
}

func (w *LeaderElectionWorker) Deinit() {
	w.setState(newUnavailableState())
	w.candidateUpdateTicker.Stop()
	w.leaderAttemptTicker.Stop()
	w.leaseRenewalTicker.Stop()
}

func (w *LeaderElectionWorker) OnDatabaseConnected() {
	w.isDatabaseConnected = true
}

func (w *LeaderElectionWorker) OnDatabaseDisconnected() {
	w.isDatabaseConnected = false
}

func (w *LeaderElectionWorker) OnSchemaUpdated() {

}

func (w *LeaderElectionWorker) DoWork() {
	w.currentState.DoWork(w)
}

func (w *LeaderElectionWorker) setState(newState ILeaderState) {
	if w.currentState != nil && w.currentState.Name() == newState.Name() {
		return
	}

	previousState := w.currentState
	w.currentState = newState
	w.currentState.OnEnterState(w, previousState)
}

func (w *LeaderElectionWorker) isAvailable() bool {
	for _, criteria := range w.leaderAvailabilityCriteria {
		if !criteria() {
			return false
		}
	}

	return true
}

func (w *LeaderElectionWorker) randomString() string {
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		Error("[LeaderElectionWorker::randomString] Failed to generate random bytes: %s", err.Error())

		return ""
	}

	r := base64.StdEncoding.EncodeToString(randomBytes)
	return r[:len(r)-1]
}

func (w *LeaderElectionWorker) isCurrentLeader() bool {
	return w.db.TempGet(w.keygen.GetLeaderKey(w.applicationName)) == w.applicationInstanceId
}

func (w *LeaderElectionWorker) updateCandidateStatus(available bool) {
	if available {
		w.db.SortedSetAdd(w.keygen.GetLeaderCandidatesKey(w.applicationName), w.applicationInstanceId, float64(time.Now().UnixNano()))
	} else {
		w.db.SortedSetRemove(w.keygen.GetLeaderCandidatesKey(w.applicationName), w.applicationInstanceId)
	}
}

func (w *LeaderElectionWorker) trimCandidates() {
	// Remove all entries except the newest MaxCandidates entries
	// Note: 0 is lowest score (oldest), -1 is highest score (newest)
	w.db.SortedSetRemoveRangeByRank(w.keygen.GetLeaderCandidatesKey(w.applicationName), 0, -(MaxCandidates + 1))
}

func (w *LeaderElectionWorker) getLeaderCandidates() []string {
	now := float64(time.Now().UnixNano())
	min := fmt.Sprintf("%f", now-float64(LeaderLeaseTimeout.Nanoseconds()))
	max := fmt.Sprintf("%f", now)

	members := w.db.SortedSetRangeByScoreWithScores(
		w.keygen.GetLeaderCandidatesKey(w.applicationName),
		min,
		max,
	)

	candidates := make([]string, len(members))
	for i, member := range members {
		candidates[i] = member.Member
	}

	return candidates
}

func (w *LeaderElectionWorker) tryBecomeLeader() bool {
	return w.db.TempSet(w.keygen.GetLeaderKey(w.applicationName), w.applicationInstanceId, LeaderLeaseTimeout)
}

func (w *LeaderElectionWorker) renewLeadershipLease() {
	w.db.TempExpire(w.keygen.GetLeaderKey(w.applicationName), LeaderLeaseTimeout)
}

func (w *LeaderElectionWorker) onBecameLeader() {
	Info("[LeaderElectionWorker::onBecameLeader] Became the leader (instanceId=%s)", w.applicationInstanceId)
	w.updateCandidateStatus(true)
	w.resetCandidateTicker()
}

func (w *LeaderElectionWorker) onBecameFollower() {
	Info("[LeaderElectionWorker::onBecameFollower] Became a follower (instanceId=%s)", w.applicationInstanceId)
	w.updateCandidateStatus(true)
	w.resetCandidateTicker()
}

func (w *LeaderElectionWorker) onBecameUnavailable() {
	Info("[LeaderElectionWorker::onBecameUnavailable] Became unavailable (instanceId=%s)", w.applicationInstanceId)
	w.updateCandidateStatus(false)
	w.resetCandidateTicker()
}

func (w *LeaderElectionWorker) setLeaderAndCandidateFields() {
	services := NewEntityFinder(w.db).Find(SearchCriteria{
		EntityType: "Service",
		Conditions: []FieldConditionEval{
			NewStringCondition().Where("ApplicationName").IsEqualTo(&String{Raw: w.applicationName}),
		},
	})

	candidates := w.getLeaderCandidates()
	for _, service := range services {
		service.GetField("Leader").PushString(w.applicationInstanceId, PushIfNotEqual)
		service.GetField("Candidates").PushString(strings.Join(candidates, ","), PushIfNotEqual)
		service.GetField("HeartbeatTrigger").PushInt(0)
	}
}

func (w *LeaderElectionWorker) clearLeaderAndCandidateFields() {
	services := NewEntityFinder(w.db).Find(SearchCriteria{
		EntityType: "Service",
		Conditions: []FieldConditionEval{
			NewStringCondition().Where("ApplicationName").IsEqualTo(&String{Raw: w.applicationName}),
		},
	})

	candidates := w.getLeaderCandidates()
	for _, service := range services {
		leaderField := service.GetField("Leader")
		if leaderField.PullString() == w.applicationInstanceId {
			leaderField.PushString("")
		}

		candidatesField := service.GetField("Candidates")
		if candidatesField.PullString() != "" {
			candidatesField.PushString(strings.Join(candidates, ","))
		}
	}
}

func (w *LeaderElectionWorker) onLosingLeadership() {
	Info("[LeaderElectionWorker::onLosingLeadership] Losing leadership status (instanceId=%s)", w.applicationInstanceId)

	w.updateCandidateStatus(false)
	w.clearLeaderAndCandidateFields()
}

func (w *LeaderElectionWorker) resetCandidateTicker() {
	w.candidateUpdateTicker.Reset(LeaderLeaseTimeout / 2)
}
