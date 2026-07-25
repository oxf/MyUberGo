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
	domain.DriverRepository
	profile       *domain.Driver
	statusChanged bool
	statusErr     error
	statusCalls   []struct{ ID, From, To string }
}

func (f *fakeProfileRepo) GetDriverByID(ctx context.Context, id string) (*domain.Driver, error) {
	return f.profile, nil
}

func (f *fakeProfileRepo) UpdateDriverStatus(ctx context.Context, id, fromStatus, toStatus string) (bool, error) {
	f.statusCalls = append(f.statusCalls, struct{ ID, From, To string }{id, fromStatus, toStatus})
	return f.statusChanged, f.statusErr
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
		profileRepo: &fakeProfileRepo{profile: &domain.Driver{Rating: 4.7}, statusChanged: true},
		outboxRepo:  outbox,
		transaction: fakeTx{},
		logger:      testLogger(),
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

func TestUpdateShift_OnlineFlipsOfflineToOnline(t *testing.T) {
	shiftRepo := &fakeShiftRepo{shift: &domain.Shift{ID: "s1", DriverID: "d1"}}
	profileRepo := &fakeProfileRepo{profile: &domain.Driver{Rating: 4.7}, statusChanged: true}
	h := &UpdateShiftHandler{
		repo:        shiftRepo,
		profileRepo: profileRepo,
		outboxRepo:  &fakeOutboxRepo{},
		transaction: fakeTx{},
		logger:      testLogger(),
	}

	if err := h.Handle(context.Background(), UpdateShift{ID: "s1", Status: "Online"}); err != nil {
		t.Fatal(err)
	}
	if len(profileRepo.statusCalls) != 1 || profileRepo.statusCalls[0] != (struct{ ID, From, To string }{"d1", "Offline", "Online"}) {
		t.Fatalf("unexpected UpdateDriverStatus call(s): %+v", profileRepo.statusCalls)
	}
}

func TestUpdateShift_EndedFlipsOnlineToOffline(t *testing.T) {
	shiftRepo := &fakeShiftRepo{shift: &domain.Shift{ID: "s1", DriverID: "d1"}}
	profileRepo := &fakeProfileRepo{profile: &domain.Driver{Rating: 4.7}, statusChanged: true}
	h := &UpdateShiftHandler{
		repo:        shiftRepo,
		profileRepo: profileRepo,
		outboxRepo:  &fakeOutboxRepo{},
		transaction: fakeTx{},
		logger:      testLogger(),
	}

	if err := h.Handle(context.Background(), UpdateShift{ID: "s1", Status: "Ended"}); err != nil {
		t.Fatal(err)
	}
	if len(profileRepo.statusCalls) != 1 || profileRepo.statusCalls[0] != (struct{ ID, From, To string }{"d1", "Online", "Offline"}) {
		t.Fatalf("unexpected UpdateDriverStatus call(s): %+v", profileRepo.statusCalls)
	}
}

func TestUpdateShift_GuardMissIsNotAnError(t *testing.T) {
	shiftRepo := &fakeShiftRepo{shift: &domain.Shift{ID: "s1", DriverID: "d1"}}
	profileRepo := &fakeProfileRepo{profile: &domain.Driver{Rating: 4.7}, statusChanged: false}
	h := &UpdateShiftHandler{
		repo:        shiftRepo,
		profileRepo: profileRepo,
		outboxRepo:  &fakeOutboxRepo{},
		transaction: fakeTx{},
		logger:      testLogger(),
	}

	// Driver is already OnRide (not Online) - the Ended guard should miss
	// without erroring, leaving the driver as-is.
	if err := h.Handle(context.Background(), UpdateShift{ID: "s1", Status: "Ended"}); err != nil {
		t.Fatalf("expected nil error on guard miss, got %v", err)
	}
}
