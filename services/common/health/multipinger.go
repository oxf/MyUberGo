package health

import "context"

// multiPinger fans a readiness check across several dependencies. The first
// error wins so the caller knows which dependency failed.
type multiPinger struct{ pingers []Pinger }

func (m multiPinger) Ping(ctx context.Context) error {
	for _, p := range m.pingers {
		if err := p.Ping(ctx); err != nil {
			return err
		}
	}
	return nil
}

// MultiPinger combines several Pingers into one, so Checker can gate
// readiness on more than one dependency (e.g. Redis + Postgres).
func MultiPinger(pingers ...Pinger) Pinger {
	return multiPinger{pingers: pingers}
}
