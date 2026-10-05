package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	app "location-service/internal/application"
	"location-service/internal/application/command"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/cache"
	"location-service/internal/infrastructure/health"
	"location-service/internal/infrastructure/metrics"
	"location-service/internal/infrastructure/pubsub"
	"location-service/internal/infrastructure/shutdown"

	"github.com/coder/websocket"
)

// instance bundles one location-service "process" worth of WS wiring —
// its own Redis client (a separate connection to the shared testcontainers
// Redis, not a shared *redis.Client), its own Dispatcher/Hub, and its own
// httptest server exposing GET /ws.
type instance struct {
	server *httptest.Server
	hub    *Hub
	cancel context.CancelFunc
}

func newInstance(t *testing.T, ownerSeed func(owner *cache.OwnerRepository)) *instance {
	t.Helper()

	rdb := newTestRedisClient(t)
	logger := testLogger()

	driverRepo := cache.NewDriverLocationRepository(rdb, 120*time.Second)
	clientRepo := cache.NewClientLocationRepository(rdb)
	trackingRepo := cache.NewTrackingRepository(rdb)
	rideTrackRepo := cache.NewRideTrackRepository(rdb)
	ownerRepo := cache.NewOwnerRepository(rdb)
	if ownerSeed != nil {
		ownerSeed(ownerRepo)
	}
	publisher := pubsub.NewRedisPublisher(rdb)

	validationConfig := domain.ValidationConfig{
		MaxAccuracyM: 100, MaxSpeedKmh: 200,
		MaxFutureSkew: 2 * time.Minute, MaxPastSkew: 10 * time.Minute,
	}
	noopMetrics := metrics.NewNoopMetricsClient()

	application := app.Application{
		Commands: app.Commands{
			IngestPings:      command.NewIngestPingsHandler(ownerRepo, driverRepo, trackingRepo, publisher, rideTrackRepo, validationConfig, logger, noopMetrics),
			IngestClientPing: command.NewIngestClientPingHandler(trackingRepo, clientRepo, publisher, rideTrackRepo, validationConfig, logger, noopMetrics),
		},
	}

	dispatcher := pubsub.NewDispatcher(rdb, logger)
	hub := NewHub(dispatcher, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Run(ctx, hub)

	// nil *sql.DB is safe here: this test never calls healthChecker.Start(),
	// so the Postgres pinger is never invoked — only WSHandler's
	// MarkNotLive/MarkNotReady calls are exercised.
	healthChecker := health.NewChecker(rdb, nil, time.Minute)
	shutdownManager := shutdown.NewManager(&http.Server{}, 5*time.Second)

	wsHandler := NewWSHandler(application, trackingRepo, ownerRepo, hub, healthChecker, shutdownManager, logger, noopMetrics, 25*time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", wsHandler.ServeWS)
	server := httptest.NewServer(mux)

	t.Cleanup(func() {
		cancel()
		server.Close()
	})

	return &instance{server: server, hub: hub, cancel: cancel}
}

func dialWS(t *testing.T, server *httptest.Server, userID, clientID string) *websocket.Conn {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, "ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("X-User-Id", userID)
	if clientID != "" {
		req.Header.Set("X-Client-Id", clientID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, req.URL.String(), &websocket.DialOptions{HTTPHeader: req.Header})
	if err != nil {
		status := "?"
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("dial ws: %v (status %s)", err, status)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "test done") })
	return conn
}

// TestHub_CrossInstanceFanOut is the proof that Slice 2's design isn't dead
// scaffolding at single-instance scale (LOCATION_SPEC.md §7.3's "single-
// instance dev works identically" claim): the driver's socket lives on
// instance A, the client's on instance B, connected via two independent
// Redis connections to the same server — single- and multi-instance take
// the identical code path here, there's no sticky-session branch to
// accidentally leave untested.
func TestHub_CrossInstanceFanOut(t *testing.T) {
	rideID := "ride-cross-instance"
	driverID := "driver-1"
	clientID := "client-1"
	driverUserID := "user-driver-1"

	instanceA := newInstance(t, func(owner *cache.OwnerRepository) {
		if err := owner.SetOwner(context.Background(), driverID, driverUserID); err != nil {
			t.Fatalf("seed owner: %v", err)
		}
	})
	instanceB := newInstance(t, nil)

	// Seed the tracking window directly (bypassing Kafka, out of this test's
	// scope) via instanceA's Redis connection — same underlying server as B.
	seedRedis := newTestRedisClient(t)
	trackingRepo := cache.NewTrackingRepository(seedRedis)
	if err := trackingRepo.RecordRideRequested(context.Background(), rideID, clientID); err != nil {
		t.Fatalf("seed requested: %v", err)
	}
	if err := trackingRepo.RecordRideAccepted(context.Background(), rideID, driverID, time.Now().UTC()); err != nil {
		t.Fatalf("seed accepted: %v", err)
	}

	driverConn := dialWS(t, instanceA.server, driverUserID, "")
	clientConn := dialWS(t, instanceB.server, "user-client-1", clientID)

	// Give both sides' Register calls (and instance A/B's SubscribeRide) a
	// beat to land before the driver pings — otherwise the publish could
	// race the client's subscription setup.
	waitFor(t, 5*time.Second, func() bool {
		return instanceA.hub.hasConnection(rideID, domain.SubjectDriver) && instanceB.hub.hasConnection(rideID, domain.SubjectClient)
	})

	now := time.Now().UTC().Truncate(time.Second)
	frame := `{"lat":34.707,"lon":33.022,"accuracyM":10,"headingDeg":90,"speedMps":5,"deviceTs":"` + now.Format(time.RFC3339) + `"}`
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := driverConn.Write(writeCtx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatalf("driver write: %v", err)
	}

	readCtx, cancelRead := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRead()
	_, data, err := clientConn.Read(readCtx)
	if err != nil {
		t.Fatalf("client read: %v", err)
	}

	if !strings.Contains(string(data), `"rideId":"`+rideID+`"`) || !strings.Contains(string(data), `"subject":"driver"`) {
		t.Fatalf("got frame %s, want a driver update for %s", data, rideID)
	}
}
