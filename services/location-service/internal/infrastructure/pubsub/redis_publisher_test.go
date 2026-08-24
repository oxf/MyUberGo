package pubsub

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"location-service/internal/domain"
)

func TestRedisPublisher_Publish_DeliversToSubscriber(t *testing.T) {
	ctx := context.Background()
	publisher := NewRedisPublisher(testRedis)

	rideID := "ride-1"
	sub := testRedis.Subscribe(ctx, rideChannel(rideID))
	defer sub.Close()
	// Block until the SUBSCRIBE has actually registered server-side — a
	// Publish sent before this would be dropped (Redis Pub/Sub has no queue).
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	update := domain.PositionUpdate{
		RideID:  rideID,
		Subject: domain.SubjectDriver,
		Position: domain.Position{
			Coordinate: domain.Coordinate{Lat: 34.707, Lon: 33.022},
			AccuracyM:  10,
			DeviceTs:   now,
			ServerTs:   now,
		},
	}

	if err := publisher.Publish(ctx, update); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case msg := <-sub.Channel():
		var got wireMessage
		if err := json.Unmarshal([]byte(msg.Payload), &got); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if got.RideID != rideID || got.Subject != "driver" || got.Lat != 34.707 || got.Lon != 33.022 {
			t.Fatalf("got %+v, want rideId=%q subject=driver lat=34.707 lon=33.022", got, rideID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for published message")
	}
}

func TestRedisPublisher_Publish_NoSubscriberIsNotAnError(t *testing.T) {
	ctx := context.Background()
	publisher := NewRedisPublisher(testRedis)

	err := publisher.Publish(ctx, domain.PositionUpdate{
		RideID:  "ride-no-subscribers",
		Subject: domain.SubjectClient,
		Position: domain.Position{
			Coordinate: domain.Coordinate{Lat: 1, Lon: 1},
			DeviceTs:   time.Now(),
			ServerTs:   time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
