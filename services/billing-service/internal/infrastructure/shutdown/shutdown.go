package shutdown

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Manager handles graceful shutdown of the HTTP server and associated resources
type Manager struct {
	server  *http.Server
	timeout time.Duration
	wg      sync.WaitGroup
	onStop  []func()
}

// NewManager creates a new shutdown manager
func NewManager(server *http.Server, timeout time.Duration) *Manager {
	return &Manager{
		server:  server,
		timeout: timeout,
	}
}

// OnStop registers a function to be called once shutdown begins, before waiting
// on the WaitGroup — typically a context cancel func that tells background
// workers (e.g. outbox/charge pollers) to stop their loop.
func (m *Manager) OnStop(fn func()) {
	m.onStop = append(m.onStop, fn)
}

// WaitForShutdown blocks until a shutdown signal is received, then gracefully shuts down the server
// It waits for all goroutines tracked by the WaitGroup to complete
func (m *Manager) WaitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	sig := <-sigChan
	log.Printf("Received signal: %v, initiating graceful shutdown...\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	if err := m.server.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	for _, fn := range m.onStop {
		fn()
	}

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All goroutines completed successfully")
	case <-ctx.Done():
		log.Println("Shutdown timeout exceeded, forcing exit")
	}

	log.Println("Graceful shutdown completed")
}

// Add increments the WaitGroup counter
func (m *Manager) Add(delta int) {
	m.wg.Add(delta)
}

// Done decrements the WaitGroup counter
func (m *Manager) Done() {
	m.wg.Done()
}

// GetWaitGroup returns the underlying sync.WaitGroup for direct usage if needed
func (m *Manager) GetWaitGroup() *sync.WaitGroup {
	return &m.wg
}
