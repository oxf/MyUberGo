package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"location-service/internal/domain"

	"github.com/coder/websocket"
)

type fakeRideSubscriber struct{}

func (fakeRideSubscriber) SubscribeRide(ctx context.Context, rideID string) error   { return nil }
func (fakeRideSubscriber) UnsubscribeRide(ctx context.Context, rideID string) error { return nil }

// TestHub_CloseRideRaceWithRegisterUnregister regression-tests the fix for a
// real data race: CloseRide previously read rc.driver/rc.client after
// releasing h.mu, racing Register/Unregister's writes under the lock. Run
// under `go test -race`, this reliably caught the bug before the fix.
func TestHub_CloseRideRaceWithRegisterUnregister(t *testing.T) {
	hub := NewHub(fakeRideSubscriber{}, testLogger())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		<-r.Context().Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	// One real connection, reused across iterations — only the Hub's own
	// locking is under test here, not message flow.
	conn := dialWS(t, server, "user-race", "")
	rideID := "ride-race"

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 500 {
			myConn := hub.Register(context.Background(), rideID, domain.SubjectDriver, conn)
			hub.Unregister(context.Background(), rideID, domain.SubjectDriver, myConn)
		}
	}()
	go func() {
		defer wg.Done()
		for range 500 {
			hub.CloseRide(rideID)
		}
	}()
	wg.Wait()
}
