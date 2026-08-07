package health

import "context"

// Pinger abstracts the underlying connectivity check (Postgres, Redis, ...)
// so Checker doesn't need to know which backend it's monitoring.
type Pinger interface {
	Ping(ctx context.Context) error
}
