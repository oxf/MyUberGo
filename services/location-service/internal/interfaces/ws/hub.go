package ws

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"location-service/internal/domain"

	"github.com/coder/websocket"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

// writeTimeout bounds a single Deliver/CloseRide/CloseAll write — a stuck
// peer socket must not block the Dispatcher goroutine or a shutdown drain.
const writeTimeout = 5 * time.Second

// RideSubscriber is the subset of pubsub.Dispatcher's API Hub needs —
// declared here (rather than importing *pubsub.Dispatcher directly) purely
// so Hub is testable against a fake without a real Redis.
type RideSubscriber interface {
	SubscribeRide(ctx context.Context, rideID string) error
	UnsubscribeRide(ctx context.Context, rideID string) error
}

// wsConn is one tracked local connection. Its identity (the pointer itself)
// is what Unregister compares against, so a stale connection's own cleanup
// can never clobber a newer connection that has already superseded it.
type wsConn struct {
	conn *websocket.Conn
}

type rideConns struct {
	driver *wsConn
	client *wsConn
}

// Hub tracks at most one driver connection and one client connection per
// rideId, subscribing/unsubscribing this instance's Dispatcher the moment a
// ride's first/last local connection appears/disappears. It has no
// awareness of message content beyond domain.PositionUpdate — decoding
// inbound frames and enforcing the rate limit is handler.go's job.
type Hub struct {
	mu         sync.Mutex
	rides      map[string]*rideConns
	dispatcher RideSubscriber
	logger     *logrus.Entry
}

func NewHub(dispatcher RideSubscriber, logger *logrus.Entry) *Hub {
	return &Hub{rides: map[string]*rideConns{}, dispatcher: dispatcher, logger: logger}
}

// Register tracks a new local connection for rideID/role. If a connection
// already existed for the same (rideID, role) — a reconnect, not a double
// open — the old one is force-closed. Returns the token Unregister must be
// called with.
func (h *Hub) Register(ctx context.Context, rideID string, role domain.SubjectType, conn *websocket.Conn) *wsConn {
	myConn := &wsConn{conn: conn}

	h.mu.Lock()
	rc, exists := h.rides[rideID]
	if !exists {
		rc = &rideConns{}
		h.rides[rideID] = rc
	}
	var stale *wsConn
	switch role {
	case domain.SubjectDriver:
		stale = rc.driver
		rc.driver = myConn
	case domain.SubjectClient:
		stale = rc.client
		rc.client = myConn
	}
	h.mu.Unlock()

	if stale != nil {
		_ = stale.conn.Close(websocket.StatusNormalClosure, "superseded by a new connection")
	}
	if !exists {
		if err := h.dispatcher.SubscribeRide(ctx, rideID); err != nil {
			h.logger.WithError(err).WithField("ride_id", rideID).Warn("hub: failed to subscribe to ride channel")
		}
	}

	return myConn
}

// Unregister removes myConn if it is still the currently-registered
// connection for (rideID, role) — a no-op if it has already been superseded
// by a reconnect's Register, so a stale connection's own cleanup can't
// clobber the newer one.
func (h *Hub) Unregister(ctx context.Context, rideID string, role domain.SubjectType, myConn *wsConn) {
	h.mu.Lock()
	rc, ok := h.rides[rideID]
	if !ok {
		h.mu.Unlock()
		return
	}

	switch role {
	case domain.SubjectDriver:
		if rc.driver != myConn {
			h.mu.Unlock()
			return
		}
		rc.driver = nil
	case domain.SubjectClient:
		if rc.client != myConn {
			h.mu.Unlock()
			return
		}
		rc.client = nil
	}

	empty := rc.driver == nil && rc.client == nil
	if empty {
		delete(h.rides, rideID)
	}
	h.mu.Unlock()

	if empty {
		if err := h.dispatcher.UnsubscribeRide(ctx, rideID); err != nil {
			h.logger.WithError(err).WithField("ride_id", rideID).Warn("hub: failed to unsubscribe from ride channel")
		}
	}
}

// Deliver implements pubsub.MessageSink — routes an accepted position to the
// *opposite* role's local connection only (a driver's own ping is never
// echoed back to the driver). A ride with no local connection for the
// counterparty (already disconnected, or this instance never had one) is a
// silent no-op, not an error — the message simply has no local recipient.
func (h *Hub) Deliver(rideID string, update domain.PositionUpdate) {
	h.mu.Lock()
	rc, ok := h.rides[rideID]
	var target *wsConn
	if ok {
		switch update.Subject {
		case domain.SubjectDriver:
			target = rc.client
		case domain.SubjectClient:
			target = rc.driver
		}
	}
	h.mu.Unlock()

	if target == nil {
		return
	}

	payload, err := json.Marshal(contracts.CounterpartyLocationUpdate{
		RideID:     rideID,
		Subject:    string(update.Subject),
		Lat:        update.Position.Coordinate.Lat,
		Lon:        update.Position.Coordinate.Lon,
		AccuracyM:  update.Position.AccuracyM,
		HeadingDeg: update.Position.HeadingDeg,
		SpeedMps:   update.Position.SpeedMps,
		DeviceTs:   update.Position.DeviceTs.UTC().Format(time.RFC3339),
		ServerTs:   update.Position.ServerTs.UTC().Format(time.RFC3339),
	})
	if err != nil {
		h.logger.WithError(err).WithField("ride_id", rideID).Error("hub: failed to encode counterparty update")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	if err := target.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		h.logger.WithError(err).WithField("ride_id", rideID).Warn("hub: failed to deliver counterparty update")
	}
}

// CloseRide force-closes both connections (if any) tracked for rideID —
// Close unblocks each read pump, which calls Unregister on its way out.
func (h *Hub) CloseRide(rideID string) {
	h.mu.Lock()
	rc, ok := h.rides[rideID]
	var driver, client *wsConn
	if ok {
		driver, client = rc.driver, rc.client
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	if driver != nil {
		_ = driver.conn.Close(websocket.StatusNormalClosure, "ride ended")
	}
	if client != nil {
		_ = client.conn.Close(websocket.StatusNormalClosure, "ride ended")
	}
}

// hasConnection reports whether a local connection is currently registered
// for (rideID, role) — used by tests to synchronize on Register having
// completed before driving a ping through it.
func (h *Hub) hasConnection(rideID string, role domain.SubjectType) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	rc, ok := h.rides[rideID]
	if !ok {
		return false
	}
	switch role {
	case domain.SubjectDriver:
		return rc.driver != nil
	case domain.SubjectClient:
		return rc.client != nil
	}
	return false
}

// CloseAll force-closes every locally-tracked connection — called from the
// shutdown manager's OnStop hook, since server.Shutdown() doesn't reach
// hijacked WS connections on its own.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	conns := make([]*wsConn, 0, len(h.rides)*2)
	for _, rc := range h.rides {
		if rc.driver != nil {
			conns = append(conns, rc.driver)
		}
		if rc.client != nil {
			conns = append(conns, rc.client)
		}
	}
	h.mu.Unlock()

	for _, c := range conns {
		_ = c.conn.Close(websocket.StatusNormalClosure, "server shutting down")
	}
}
