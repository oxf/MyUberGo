package pubsub

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"location-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type fakeSink struct {
	mu        sync.Mutex
	delivered []domain.PositionUpdate
}

func (f *fakeSink) Deliver(rideID string, update domain.PositionUpdate) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delivered = append(f.delivered, update)
}

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.delivered)
}

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

func TestDispatcher_DeliversOnlyForSubscribedRides(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &fakeSink{}
	dispatcher := NewDispatcher(testRedis, testLogger())
	go dispatcher.Run(ctx, sink)

	if err := dispatcher.SubscribeRide(ctx, "ride-subscribed"); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Give go-redis a beat to actually register the SUBSCRIBE server-side —
	// there's no synchronous confirmation for a *Client.Publish the way
	// *PubSub.Receive gives one for a direct SUBSCRIBE.
	time.Sleep(200 * time.Millisecond)

	now := time.Now().UTC().Truncate(time.Second)
	publisher := NewRedisPublisher(testRedis)
	if err := publisher.Publish(ctx, domain.PositionUpdate{
		RideID:  "ride-subscribed",
		Subject: domain.SubjectDriver,
		Position: domain.Position{
			Coordinate: domain.Coordinate{Lat: 34.707, Lon: 33.022},
			DeviceTs:   now,
			ServerTs:   now,
		},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Never subscribed — must never reach the sink.
	if err := publisher.Publish(ctx, domain.PositionUpdate{
		RideID:  "ride-never-subscribed",
		Subject: domain.SubjectDriver,
		Position: domain.Position{
			Coordinate: domain.Coordinate{Lat: 1, Lon: 1},
			DeviceTs:   now,
			ServerTs:   now,
		},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && sink.count() == 0 {
		time.Sleep(50 * time.Millisecond)
	}

	if sink.count() != 1 {
		t.Fatalf("got %d delivered updates, want 1", sink.count())
	}
	if sink.delivered[0].RideID != "ride-subscribed" {
		t.Fatalf("got rideID %q, want ride-subscribed", sink.delivered[0].RideID)
	}
}

func TestDispatcher_UnsubscribeStopsFurtherDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &fakeSink{}
	dispatcher := NewDispatcher(testRedis, testLogger())
	go dispatcher.Run(ctx, sink)

	rideID := "ride-unsub"
	if err := dispatcher.SubscribeRide(ctx, rideID); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := dispatcher.UnsubscribeRide(ctx, rideID); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	now := time.Now().UTC()
	publisher := NewRedisPublisher(testRedis)
	if err := publisher.Publish(ctx, domain.PositionUpdate{
		RideID:   rideID,
		Subject:  domain.SubjectDriver,
		Position: domain.Position{Coordinate: domain.Coordinate{Lat: 1, Lon: 1}, DeviceTs: now, ServerTs: now},
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	time.Sleep(1 * time.Second)
	if sink.count() != 0 {
		t.Fatalf("got %d delivered updates after unsubscribe, want 0", sink.count())
	}
}
