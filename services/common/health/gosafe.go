package health

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

// GoSafe runs fn in a goroutine, recovering panics. If workerCtx is non-nil
// and fn returns before it's cancelled, that's a genuine liveness failure.
func GoSafe(logger *logrus.Entry, checker *Checker, workerCtx context.Context, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("goroutine", name).WithField("panic", r).Error("recovered from panic")
			}
			if workerCtx != nil && workerCtx.Err() == nil {
				checker.MarkNotLive(name + " exited unexpectedly")
			}
		}()
		fn()
	}()
}

// HealthcheckSelf backs the `app healthcheck` subcommand: a plain HTTP GET
// against the service's own readiness endpoint, exiting 0/1 for Docker.
func HealthcheckSelf(url string) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}
