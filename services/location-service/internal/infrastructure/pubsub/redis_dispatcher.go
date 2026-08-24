package pubsub

import (
	"context"
	"sync"

	"location-service/internal/domain"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// MessageSink receives a decoded position update for a ride this instance
// holds a local WS connection for — implemented by internal/interfaces/ws.Hub.
type MessageSink interface {
	Deliver(rideID string, update domain.PositionUpdate)
}

// Dispatcher is the cross-instance Pub/Sub fan-out from LOCATION_SPEC.md
// §7.3. The sink is passed to Run, not the constructor, since Hub needs a
// constructed Dispatcher first — see cmd/main.go's wiring order.
type Dispatcher struct {
	ps         *redis.PubSub
	logger     *logrus.Entry
	mu         sync.Mutex
	subscribed map[string]bool
}

// NewDispatcher opens the one subscription object for this instance's
// lifetime, initially subscribed to nothing.
func NewDispatcher(rdb *redis.Client, logger *logrus.Entry) *Dispatcher {
	return &Dispatcher{
		ps:         rdb.Subscribe(context.Background()),
		logger:     logger,
		subscribed: map[string]bool{},
	}
}

// SubscribeRide is idempotent; the Redis round trip runs outside d.mu so
// concurrent rides don't serialize behind one lock held across network I/O.
func (d *Dispatcher) SubscribeRide(ctx context.Context, rideID string) error {
	d.mu.Lock()
	already := d.subscribed[rideID]
	d.mu.Unlock()
	if already {
		return nil
	}

	if err := d.ps.Subscribe(ctx, rideChannel(rideID)); err != nil {
		return err
	}

	d.mu.Lock()
	d.subscribed[rideID] = true
	d.mu.Unlock()
	return nil
}

// UnsubscribeRide mirrors SubscribeRide's lock-scope reasoning.
func (d *Dispatcher) UnsubscribeRide(ctx context.Context, rideID string) error {
	d.mu.Lock()
	subscribed := d.subscribed[rideID]
	d.mu.Unlock()
	if !subscribed {
		return nil
	}

	if err := d.ps.Unsubscribe(ctx, rideChannel(rideID)); err != nil {
		return err
	}

	d.mu.Lock()
	delete(d.subscribed, rideID)
	d.mu.Unlock()
	return nil
}

// Run ranges over the subscription's message channel until ctx is
// cancelled. A malformed payload is logged and skipped, same reasoning as
// kafkaconsumer's poison-message handling.
func (d *Dispatcher) Run(ctx context.Context, sink MessageSink) {
	defer d.ps.Close()
	ch := d.ps.Channel()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			update, err := decodeWireMessage([]byte(msg.Payload))
			if err != nil {
				d.logger.WithError(err).WithField("channel", msg.Channel).Warn("dispatcher: failed to decode pubsub payload")
				continue
			}
			sink.Deliver(update.RideID, update)
		}
	}
}
