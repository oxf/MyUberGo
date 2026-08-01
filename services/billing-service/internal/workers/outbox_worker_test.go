package workers

import (
	"billing-service/internal/domain"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakePublisher lets tests script per-topic publish outcomes without a real
// Kafka broker — OutboxWorker only ever talks to services.EventPublisher.
type fakePublisher struct {
	mu      sync.Mutex
	calls   []string
	failFor map[string]bool
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{failFor: make(map[string]bool)}
}

func (p *fakePublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, topic)
	if p.failFor[topic] {
		return errors.New("publish failed")
	}
	return nil
}

func (p *fakePublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func insertOutboxMessage(t *testing.T, repo *fakeOutboxRepo, id, topic string, retries int) {
	t.Helper()
	if err := repo.Insert(context.Background(), &domain.OutboxMessage{
		ID: id, Topic: topic, EventType: topic, Payload: []byte(`{}`), Retries: retries,
	}); err != nil {
		t.Fatalf("insert outbox message: %v", err)
	}
}

func TestOutboxWorker_ProcessBatch_SuccessMarksProcessed(t *testing.T) {
	repo := newFakeOutboxRepo()
	insertOutboxMessage(t, repo, "msg-1", "payment.completed", 0)
	publisher := newFakePublisher()
	worker := NewOutboxWorker(repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	msg := repo.get("msg-1")
	if !msg.Processed {
		t.Fatal("expected message to be marked processed")
	}
	if msg.Retries != 0 {
		t.Fatalf("expected retries unchanged, got %d", msg.Retries)
	}
	if publisher.callCount() != 1 {
		t.Fatalf("expected exactly one publish call, got %d", publisher.callCount())
	}
}

func TestOutboxWorker_ProcessBatch_PublishFailureIncrementsRetries(t *testing.T) {
	repo := newFakeOutboxRepo()
	insertOutboxMessage(t, repo, "msg-1", "payment.failed", 2)
	publisher := newFakePublisher()
	publisher.failFor["payment.failed"] = true
	worker := NewOutboxWorker(repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	msg := repo.get("msg-1")
	if msg.Processed {
		t.Fatal("expected message to remain unprocessed after a failed publish")
	}
	if msg.Retries != 3 {
		t.Fatalf("expected retries incremented to 3, got %d", msg.Retries)
	}
}

func TestOutboxWorker_ProcessBatch_MaxRetriesParksMessage(t *testing.T) {
	repo := newFakeOutboxRepo()
	insertOutboxMessage(t, repo, "msg-1", "payment.completed", defaultMaxRetries)
	publisher := newFakePublisher()
	worker := NewOutboxWorker(repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	msg := repo.get("msg-1")
	if msg.Processed {
		t.Fatal("a parked message must not be marked processed")
	}
	if msg.Retries != defaultMaxRetries {
		t.Fatalf("a parked message's retries must not increment further, got %d", msg.Retries)
	}
	if publisher.callCount() != 0 {
		t.Fatalf("a parked message must never reach the publisher, got %d calls", publisher.callCount())
	}
}

func TestOutboxWorker_ProcessBatch_MixedBatch(t *testing.T) {
	repo := newFakeOutboxRepo()
	insertOutboxMessage(t, repo, "ok", "payment.completed", 0)
	insertOutboxMessage(t, repo, "retry", "payment.failed", 0)
	insertOutboxMessage(t, repo, "parked", "payment.completed", defaultMaxRetries)
	publisher := newFakePublisher()
	publisher.failFor["payment.failed"] = true
	worker := NewOutboxWorker(repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if !repo.get("ok").Processed {
		t.Fatal("expected 'ok' message processed")
	}
	if repo.get("retry").Processed || repo.get("retry").Retries != 1 {
		t.Fatalf("expected 'retry' message unprocessed with retries=1, got processed=%v retries=%d",
			repo.get("retry").Processed, repo.get("retry").Retries)
	}
	if repo.get("parked").Processed || repo.get("parked").Retries != defaultMaxRetries {
		t.Fatalf("expected 'parked' message untouched, got processed=%v retries=%d",
			repo.get("parked").Processed, repo.get("parked").Retries)
	}
	// ok + retry only — parked must never reach the publisher.
	if publisher.callCount() != 2 {
		t.Fatalf("expected 2 publish calls (ok + retry), got %d", publisher.callCount())
	}
}
