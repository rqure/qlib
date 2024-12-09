package leadership

type State interface {
	DoWork(Candidate)
	OnEnterState(Candidate, State)
	Name() string
}
