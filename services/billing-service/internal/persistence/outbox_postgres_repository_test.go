package persistence

import (
	"billing-service/internal/domain"
	"context"
	"fmt"
	"testing"
)

// TestGetUnprocessedBatch_SkipLockedNoDoubleClaim exercises the real "FOR
// UPDATE SKIP LOCKED" guarantee under two genuinely concurrent transactions
// — no fake/mock repository can verify this.
func TestGetUnprocessedBatch_SkipLockedNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresOutboxRepository(testDB)

	for range 6 {
		msg := &domain.OutboxMessage{
			Topic:     "payment.completed",
			EventType: "PaymentCompleted",
			Payload:   []byte(`{}`),
		}
		if err := repo.Insert(ctx, msg); err != nil {
			t.Fatalf("insert outbox message: %v", err)
		}
	}

	tx1, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer func() { _ = tx1.Rollback() }()

	batch1, err := repo.GetUnprocessedBatch(WithTx(ctx, tx1), 3)
	if err != nil {
		t.Fatalf("tx1 GetUnprocessedBatch: %v", err)
	}
	if len(batch1) != 3 {
		t.Fatalf("expected 3 rows in batch1, got %d", len(batch1))
	}

	tx2, err := testDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer func() { _ = tx2.Rollback() }()

	batch2, err := repo.GetUnprocessedBatch(WithTx(ctx, tx2), 3)
	if err != nil {
		t.Fatalf("tx2 GetUnprocessedBatch: %v", err)
	}
	if len(batch2) != 3 {
		t.Fatalf("expected 3 rows in batch2 (skipping tx1's locked rows), got %d", len(batch2))
	}

	seen := map[string]bool{}
	for _, m := range batch1 {
		seen[m.ID] = true
	}
	for _, m := range batch2 {
		if seen[m.ID] {
			t.Fatalf("batch2 claimed row %s already claimed by batch1 — SKIP LOCKED failed to exclude it", m.ID)
		}
	}
}

func TestInsert_MarkProcessed_IncrementRetries(t *testing.T) {
	ctx := context.Background()
	repo := NewPostgresOutboxRepository(testDB)

	topic := fmt.Sprintf("test.topic.%d", nextSeq())
	msg := &domain.OutboxMessage{Topic: topic, EventType: "TestEvent", Payload: []byte(`{}`)}
	if err := repo.Insert(ctx, msg); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	var id string
	if err := testDB.QueryRow(`SELECT id FROM billing.outbox_message WHERE topic = $1`, topic).Scan(&id); err != nil {
		t.Fatalf("find inserted row by topic: %v", err)
	}

	var processed bool
	var retries int
	if err := testDB.QueryRowContext(ctx, `SELECT processed, retries FROM billing.outbox_message WHERE id = $1`, id).Scan(&processed, &retries); err != nil {
		t.Fatalf("read inserted row: %v", err)
	}
	if processed || retries != 0 {
		t.Fatalf("expected fresh row processed=false retries=0, got processed=%v retries=%d", processed, retries)
	}

	if err := repo.IncrementRetries(ctx, id); err != nil {
		t.Fatalf("IncrementRetries: %v", err)
	}
	if err := testDB.QueryRowContext(ctx, `SELECT retries FROM billing.outbox_message WHERE id = $1`, id).Scan(&retries); err != nil {
		t.Fatalf("read retries: %v", err)
	}
	if retries != 1 {
		t.Fatalf("expected retries=1 after IncrementRetries, got %d", retries)
	}

	if err := repo.MarkProcessed(ctx, id); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}
	if err := testDB.QueryRowContext(ctx, `SELECT processed FROM billing.outbox_message WHERE id = $1`, id).Scan(&processed); err != nil {
		t.Fatalf("read processed: %v", err)
	}
	if !processed {
		t.Fatal("expected processed=true after MarkProcessed")
	}
}
