package health

import (
	"context"
	"database/sql"
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
	db            *sql.DB
	state         State
	mu            sync.RWMutex
	checkInterval time.Duration
	queryTimeout  time.Duration
	stopChan      chan struct{}
}

// NewChecker creates a new health checker
func NewChecker(db *sql.DB, checkInterval time.Duration) *Checker {
	return &Checker{
		db:            db,
		state:         State{Live: true, Ready: false},
		checkInterval: checkInterval,
		queryTimeout:  2 * time.Second,
		stopChan:      make(chan struct{}),
	}
}

// Start begins the background health check goroutine
func (c *Checker) Start() {
	ticker := time.NewTicker(c.checkInterval)
	defer ticker.Stop()

	// Initial health check
	c.updateReady()

	go func() {
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

// updateReady performs a database connectivity check and updates ready state
func (c *Checker) updateReady() {
	ctx, cancel := context.WithTimeout(context.Background(), c.queryTimeout)
	defer cancel()

	err := c.db.PingContext(ctx)
	c.mu.Lock()
	c.state.Ready = (err == nil)
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

	state := c.GetState()
	w.Header().Set("Content-Type", "application/json")

	if state.Live {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"live":true}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"live":false}`))
	}
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
