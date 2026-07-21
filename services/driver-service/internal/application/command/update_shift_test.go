package command

import (
	"context"
	"encoding/json"
	"testing"

	"driver-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/kafka"
)

type fakeShiftRepo struct {
	domain.ShiftRepository
	shift *domain.Shift
	ended []string
}

func (f *fakeShiftRepo) GetShiftByID(ctx context.Context, id string) (*domain.Shift, error) {
	return f.shift, nil
}
func (f *fakeShiftRepo) UpdateShift(ctx context.Context, s *domain.Shift) error { return nil }
func (f *fakeShiftRepo) EndShift(ctx context.Context, id string) error {
	f.ended = append(f.ended, id)
	return nil
}

type fakeProfileRepo struct {
	domain.DriverProfileRepository
	profile *domain.DriverProfile
}

func (f *fakeProfileRepo) GetDriverProfileByID(ctx context.Context, id string) (*domain.DriverProfile, error) {
	return f.profile, nil
}

type fakeOutboxRepo struct {
	domain.OutboxRepository
	inserted []*domain.OutboxMessage
}

func (f *fakeOutboxRepo) Insert(ctx context.Context, m *domain.OutboxMessage) error {
	f.inserted = append(f.inserted, m)
	return nil
}

type fakeTx struct{}

func (fakeTx) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func TestUpdateShift_EndedEmitsEventWithRating(t *testing.T) {
	shiftRepo := &fakeShiftRepo{shift: &domain.Shift{ID: "s1", DriverID: "d1"}}
	outbox := &fakeOutboxRepo{}
	h := &UpdateShiftHandler{
		repo:        shiftRepo,
		profileRepo: &fakeProfileRepo{profile: &domain.DriverProfile{Rating: 4.7}},
		outboxRepo:  outbox,
		transaction: fakeTx{},
	}

	if err := h.Handle(context.Background(), UpdateShift{ID: "s1", Status: "Ended"}); err != nil {
		t.Fatal(err)
	}

	if len(shiftRepo.ended) != 1 || shiftRepo.ended[0] != "s1" {
		t.Fatalf("EndShift not called for s1: %v", shiftRepo.ended)
	}
	if len(outbox.inserted) != 1 {
		t.Fatalf("expected 1 outbox message for Ended shift, got %d", len(outbox.inserted))
	}
	var ev contracts.ShiftUpdatedEvent
	if err := json.Unmarshal(outbox.inserted[0].Payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Status != "Ended" || ev.DriverID != "d1" || ev.Rating != 4.7 {
		t.Fatalf("bad event: %+v", ev)
	}
}
