package app

type Application interface {
	Execute()
	Quit()
}

type Worker interface {
	Deinit()
	DoWork()
	Init()
}
