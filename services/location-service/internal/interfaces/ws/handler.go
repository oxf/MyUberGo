package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	app "location-service/internal/application"
	"location-service/internal/application/command"
	"location-service/internal/common/decorator"
	"location-service/internal/domain"
	"location-service/internal/infrastructure/health"
	"location-service/internal/infrastructure/shutdown"

	"github.com/coder/websocket"
	"github.com/oxf/MyUber/common/kongheaders"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/time/rate"
)

const (
	// readLimitBytes bounds one inbound frame — single-ping-per-frame, no batching over WS.
	readLimitBytes = 4096
	// rateLimitPerSecond/rateBurst: the in-process cap Kong can't enforce post-upgrade.
	rateLimitPerSecond rate.Limit = 2
	rateBurst          int        = 10
)

// WSHandler serves GET /ws. Identity is bound once at handshake from Kong
// headers — rideId/role are then resolved server-side, never client-supplied.
type WSHandler struct {
	ingestor        app.LocationIngestor
	tracking        domain.TrackingRepository
	owner           domain.OwnerRepository
	hub             *Hub
	healthChecker   *health.Checker
	shutdownManager *shutdown.Manager
	logger          *logrus.Entry
	metrics         decorator.MetricsClient
	pingInterval    time.Duration
}

func NewWSHandler(
	ingestor app.LocationIngestor,
	tracking domain.TrackingRepository,
	owner domain.OwnerRepository,
	hub *Hub,
	healthChecker *health.Checker,
	shutdownManager *shutdown.Manager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
	pingInterval time.Duration,
) *WSHandler {
	return &WSHandler{
		ingestor: ingestor, tracking: tracking, owner: owner, hub: hub,
		healthChecker: healthChecker, shutdownManager: shutdownManager,
		logger: logger, metrics: metricsClient, pingInterval: pingInterval,
	}
}

func (h *WSHandler) ServeWS(w http.ResponseWriter, r *http.Request) {
	userID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}
	clientID, _ := kongheaders.ClientID(r)

	role, subjectID, rideID, err := h.resolveSubject(r.Context(), userID, clientID)
	if err != nil {
		h.logger.WithError(err).Warn("ws: failed to resolve caller's tracking window")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if rideID == "" {
		http.Error(w, "no open tracking window for this caller", http.StatusForbidden)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		// websocket.Accept has already written a response on failure.
		return
	}
	conn.SetReadLimit(readLimitBytes)

	myConn := h.hub.Register(r.Context(), rideID, role, conn)
	// Closed by readPump's cleanup so heartbeatPump exits immediately on
	// disconnect, instead of waiting out its next ping tick.
	done := make(chan struct{})

	h.shutdownManager.Add(1)
	health.GoSafe(h.logger, h.healthChecker, nil, "ws-read-pump", func() {
		defer h.shutdownManager.Done()
		defer close(done)
		h.readPump(conn, myConn, rideID, role, subjectID)
	})

	h.shutdownManager.Add(1)
	health.GoSafe(h.logger, h.healthChecker, nil, "ws-heartbeat-pump", func() {
		defer h.shutdownManager.Done()
		h.heartbeatPump(conn, done)
	})
}

// resolveSubject tries the client identity first, then the driver identity
// via the cached owner mapping. Empty rideID with a nil error means "no open
// window" — the handler's 403 case.
func (h *WSHandler) resolveSubject(ctx context.Context, userID, clientID string) (role domain.SubjectType, subjectID string, rideID string, err error) {
	if clientID != "" {
		rideID, err = h.tracking.ActiveRideForClient(ctx, clientID)
		if err != nil {
			return "", "", "", err
		}
		if rideID != "" {
			return domain.SubjectClient, clientID, rideID, nil
		}
	}

	driverID, err := h.owner.DriverIDForUser(ctx, userID)
	if err != nil {
		return "", "", "", err
	}
	if driverID == "" {
		return "", "", "", nil
	}
	rideID, err = h.tracking.ActiveRideForDriver(ctx, driverID)
	if err != nil {
		return "", "", "", err
	}
	if rideID == "" {
		return "", "", "", nil
	}
	return domain.SubjectDriver, userID, rideID, nil
}

// readPump owns myConn's lifecycle, always ending in Unregister + CloseNow —
// Read returns an error on any disconnect cause, clean or forced.
func (h *WSHandler) readPump(conn *websocket.Conn, myConn *wsConn, rideID string, role domain.SubjectType, subjectID string) {
	defer func() {
		h.hub.Unregister(context.Background(), rideID, role, myConn)
		_ = conn.CloseNow()
	}()

	limiter := rate.NewLimiter(rateLimitPerSecond, rateBurst)
	ctx := context.Background()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}

		// A merely-bursty client keeps its connection — drop the frame, not the socket.
		if !limiter.Allow() {
			h.metrics.IncCounter(ctx, "myubergo.location.pings_rejected", attribute.String("reason", "ws_rate_limited"))
			continue
		}

		var frame contracts.LocationUpdateFrame
		if err := json.Unmarshal(data, &frame); err != nil {
			h.metrics.IncCounter(ctx, "myubergo.location.pings_rejected", attribute.String("reason", "ws_malformed_frame"))
			continue
		}
		deviceTs, err := time.Parse(time.RFC3339, frame.DeviceTs)
		if err != nil {
			h.metrics.IncCounter(ctx, "myubergo.location.pings_rejected", attribute.String("reason", "ws_malformed_frame"))
			continue
		}

		ping := command.PingInput{
			Lat: frame.Lat, Lon: frame.Lon, AccuracyM: frame.AccuracyM,
			HeadingDeg: frame.HeadingDeg, SpeedMps: frame.SpeedMps, DeviceTs: deviceTs,
		}

		switch role {
		case domain.SubjectDriver:
			if _, err := h.ingestor.IngestDriverPings(ctx, command.IngestPings{UserID: subjectID, Pings: []command.PingInput{ping}}); err != nil {
				h.logger.WithError(err).WithField("ride_id", rideID).Warn("ws: driver ping ingest failed")
			}
		case domain.SubjectClient:
			if _, err := h.ingestor.IngestClientPing(ctx, command.IngestClientPing{ClientID: subjectID, Ping: ping}); err != nil {
				h.logger.WithError(err).WithField("ride_id", rideID).Warn("ws: client ping ingest failed")
			}
		}
	}
}

// heartbeatPump sends WS pings so Kong's 60s proxy_read_timeout doesn't kill
// an idle connection, exiting promptly on either a failed ping or done.
func (h *WSHandler) heartbeatPump(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(h.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			err := conn.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
