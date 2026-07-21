package command

import (
	"context"
	"testing"
)

func TestProcessRideAccepted_ReturnsNilForWellFormedCommand(t *testing.T) {
	h := &ProcessRideAcceptedHandler{}

	err := h.Handle(context.Background(), ProcessRideAccepted{
		RideID:     "ride-1",
		DriverID:   "driver-1",
		AcceptedAt: "2026-07-21T10:00:00Z",
	})
	if err != nil {
		t.Fatalf("expected nil error from placeholder handler, got %v", err)
	}
}
