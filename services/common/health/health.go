package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// State represents the health state of the service
type State struct {
	Live  bool `json:"live"`
	Ready bool `json:"ready"`
}

// Checker monitors the health of the service
type Checker struct {
	pinger        Pinger
	state         State
	notLiveReason string
	shuttingDown  bool
	mu            sync.RWMutex
	checkInterval time.Duration
	queryTimeout  time.Duration
	stopChan      chan struct{}
}

// NewChecker creates a new health checker
func NewChecker(pinger Pinger, checkInterval time.Duration) *Checker {
	return &Checker{
		pinger:        pinger,
		state:         State{Live: true, Ready: false},
		checkInterval: checkInterval,
		queryTimeout:  2 * time.Second,
		stopChan:      make(chan struct{}),
	}
}

// MarkNotLive permanently flips Live to false: a background goroutine exited
// without being told to stop. Idempotent — only the first reason is kept.
func (c *Checker) MarkNotLive(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.state.Live {
		return
	}
	c.state.Live = false
	c.notLiveReason = reason
}

// MarkNotReady flips Ready to false immediately on shutdown, and latches
// shuttingDown so a later successful ping can't flip Ready back to true.
func (c *Checker) MarkNotReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.Ready = false
	c.shuttingDown = true
}

// Start begins the background health check goroutine
func (c *Checker) Start() {
	// Initial health check
	c.updateReady()

	go func() {
		ticker := time.NewTicker(c.checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.updateReady()
			case <-c.stopChan:
				return
			}
		}
	}()
}

// Stop stops the background health check goroutine
func (c *Checker) Stop() {
	close(c.stopChan)
}

// updateReady pings the backend via Pinger and updates ready state.
func (c *Checker) updateReady() {
	c.mu.RLock()
	shuttingDown := c.shuttingDown
	c.mu.RUnlock()
	if shuttingDown {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.queryTimeout)
	defer cancel()

	err := c.pinger.Ping(ctx)

	c.mu.Lock()
	if !c.shuttingDown {
		c.state.Ready = (err == nil)
	}
	c.mu.Unlock()
}

// GetState returns a copy of the current health state
func (c *Checker) GetState() State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state
}

// LiveHandler handles GET /health/live requests
func (c *Checker) LiveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	c.mu.RLock()
	state, reason := c.state, c.notLiveReason
	c.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	body := map[string]any{"live": state.Live}
	if !state.Live && reason != "" {
		// Surfaced here so a probe/operator hitting /health/live directly
		// doesn't have to go correlate logs to find out why.
		body["reason"] = reason
	}
	if state.Live {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(body)
}

// ReadyHandler handles GET /health/ready requests
func (c *Checker) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	state := c.GetState()
	w.Header().Set("Content-Type", "application/json")

	if state.Ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ready":true}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"ready":false}`))
	}
}
