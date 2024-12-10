package signalslots

type Slot interface {
	Invoke(...interface{})
}

type Signal interface {
	Connect(Slot)
	Disconnect(Slot)
	DisconnectAll()
	Emit(...interface{})
}
