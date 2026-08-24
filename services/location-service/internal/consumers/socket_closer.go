package consumers

// SocketCloser terminates locally-tracked WS connections for a ride —
// satisfied by internal/interfaces/ws.Hub, declared here to avoid an import cycle.
type SocketCloser interface {
	CloseRide(rideID string)
}

// noopSocketCloser is the Stage 1 stand-in, wired to the real Hub in Stage 4's
// cmd/main.go change.
type noopSocketCloser struct{}

func (noopSocketCloser) CloseRide(string) {}

// NoopSocketCloser is the default SocketCloser until the WS hub is wired in.
func NoopSocketCloser() SocketCloser { return noopSocketCloser{} }
