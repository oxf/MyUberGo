package ws

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"location-service/internal/domain"
	"location-service/internal/infrastructure/cache"

	"github.com/coder/websocket"
)

func TestWSHandler_NoOpenWindowIsRejectedBeforeUpgrade(t *testing.T) {
	instance := newInstance(t, nil)

	req, err := http.NewRequest(http.MethodGet, "ws"+strings.TrimPrefix(instance.server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-User-Id", "user-no-window")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, req.URL.String(), &websocket.DialOptions{HTTPHeader: req.Header})
	if err == nil {
		t.Fatal("expected dial to fail: no open tracking window")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		status := "?"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("got status %s, want 403", status)
	}
}

// TestWSHandler_MalformedFrameIsDroppedNotFatal sends a malformed frame
// followed by a valid one — if the malformed frame killed the connection,
// the valid one would never arrive at the counterparty.
func TestWSHandler_MalformedFrameIsDroppedNotFatal(t *testing.T) {
	rideID := "ride-malformed"
	driverID := "driver-malformed"
	clientID := "client-malformed"
	driverUserID := "user-driver-malformed"

	instanceA := newInstance(t, func(owner *cache.OwnerRepository) {
		if err := owner.SetOwner(context.Background(), driverID, driverUserID); err != nil {
			t.Fatalf("seed owner: %v", err)
		}
	})
	instanceB := newInstance(t, nil)

	seedRedis := newTestRedisClient(t)
	trackingRepo := cache.NewTrackingRepository(seedRedis)
	if err := trackingRepo.RecordRideRequested(context.Background(), rideID, clientID); err != nil {
		t.Fatalf("seed requested: %v", err)
	}
	if err := trackingRepo.RecordRideAccepted(context.Background(), rideID, driverID, time.Now().UTC()); err != nil {
		t.Fatalf("seed accepted: %v", err)
	}

	driverConn := dialWS(t, instanceA.server, driverUserID, "")
	clientConn := dialWS(t, instanceB.server, "user-client-malformed", clientID)

	waitFor(t, 5*time.Second, func() bool {
		return instanceA.hub.hasConnection(rideID, domain.SubjectDriver) && instanceB.hub.hasConnection(rideID, domain.SubjectClient)
	})

	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := driverConn.Write(writeCtx, websocket.MessageText, []byte(`not json`)); err != nil {
		t.Fatalf("write malformed frame: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	valid := `{"lat":34.707,"lon":33.022,"accuracyM":10,"headingDeg":90,"speedMps":5,"deviceTs":"` + now.Format(time.RFC3339) + `"}`
	if err := driverConn.Write(writeCtx, websocket.MessageText, []byte(valid)); err != nil {
		t.Fatalf("write valid frame: %v", err)
	}

	readCtx, cancelRead := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRead()
	_, data, err := clientConn.Read(readCtx)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if !strings.Contains(string(data), `"rideId":"`+rideID+`"`) {
		t.Fatalf("got frame %s, want a driver update for %s", data, rideID)
	}
}
