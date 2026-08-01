package persistence

import (
	commonerrors "billing-service/internal/common/errors"
	"billing-service/internal/domain"
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestInsert_DuplicateID_ReturnsErrDuplicatePspEvent(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPspEventRepository(testDB)

	id := fmt.Sprintf("evt_test_%d", nextSeq())
	event := &domain.PspEvent{ID: id, Type: "payment_intent.succeeded", APIVersion: "2026-01-01", Payload: []byte(`{}`)}

	if err := repo.Insert(ctx, event); err != nil {
		t.Fatalf("first Insert: %v", err)
	}

	err := repo.Insert(ctx, event)
	if !errors.Is(err, domain.ErrDuplicatePspEvent) {
		t.Fatalf("expected ErrDuplicatePspEvent for a redelivered webhook event id, got %v", err)
	}
}

func TestMarkProcessed_ClearsProcessError(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPspEventRepository(testDB)

	id := fmt.Sprintf("evt_test_%d", nextSeq())
	event := &domain.PspEvent{ID: id, Type: "payment_intent.payment_failed", APIVersion: "2026-01-01", Payload: []byte(`{}`)}
	if err := repo.Insert(ctx, event); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if _, err := testDB.ExecContext(ctx, `UPDATE billing.psp_event SET process_error = 'boom' WHERE id = $1`, id); err != nil {
		t.Fatalf("seed process_error: %v", err)
	}

	if err := repo.MarkProcessed(ctx, id); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	got, err := repo.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ProcessedAt == nil {
		t.Fatal("expected ProcessedAt to be set")
	}
	if got.ProcessError != nil {
		t.Fatalf("expected MarkProcessed to also clear process_error, got %q", *got.ProcessError)
	}
}

func TestGetByID_NotFound_PspEvent(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresPspEventRepository(testDB)

	_, err := repo.GetByID(ctx, "evt_does_not_exist")
	if !errors.Is(err, commonerrors.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
