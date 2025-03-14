package qss

type Slot interface {
	Invoke(...interface{})
}

type Signal interface {
	Connect(interface{})
	Disconnect(Slot)
	DisconnectAll()
	Emit(...interface{})
}
