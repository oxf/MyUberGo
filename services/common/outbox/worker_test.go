package outbox

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func testLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

// fakeRepo is an in-memory Repository — no DB, no network.
type fakeRepo struct {
	mu       sync.Mutex
	messages []*Message
}

func newFakeRepo() *fakeRepo { return &fakeRepo{} }

func (r *fakeRepo) insert(m *Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, m)
}

func (r *fakeRepo) GetUnprocessedBatch(ctx context.Context, limit int) ([]*Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var result []*Message
	for _, m := range r.messages {
		if m.Processed {
			continue
		}
		if m.ClaimedUntil != nil {
			claimedUntil, err := time.Parse(time.RFC3339, *m.ClaimedUntil)
			if err == nil && claimedUntil.After(time.Now().UTC()) {
				continue
			}
		}
		result = append(result, m)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (r *fakeRepo) SetClaimedUntil(ctx context.Context, id string, claimedUntil string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			m.ClaimedUntil = &claimedUntil
			return nil
		}
	}
	return errors.New("not found")
}

func (r *fakeRepo) MarkProcessed(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			m.Processed = true
			return nil
		}
	}
	return errors.New("not found")
}

func (r *fakeRepo) IncrementRetries(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			m.Retries++
			return nil
		}
	}
	return errors.New("not found")
}

func (r *fakeRepo) get(id string) *Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.messages {
		if m.ID == id {
			return m
		}
	}
	return nil
}

// fakePublisher lets tests script per-topic publish outcomes without a real Kafka broker.
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

// fakeTransactionManager has no real transactional semantics — nothing to roll back
// against an in-memory fake — it exists only to satisfy TransactionManager.
type fakeTransactionManager struct{}

func (fakeTransactionManager) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func insertMessage(repo *fakeRepo, id, topic string, retries int) {
	repo.insert(&Message{ID: id, Topic: topic, EventType: topic, Payload: []byte(`{}`), Retries: retries})
}

func TestWorker_ProcessBatch_SuccessMarksProcessed(t *testing.T) {
	repo := newFakeRepo()
	insertMessage(repo, "msg-1", "payment.completed", 0)
	publisher := newFakePublisher()
	worker := New("test-service", repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

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

func TestWorker_ProcessBatch_PublishFailureIncrementsRetries(t *testing.T) {
	repo := newFakeRepo()
	insertMessage(repo, "msg-1", "payment.failed", 2)
	publisher := newFakePublisher()
	publisher.failFor["payment.failed"] = true
	worker := New("test-service", repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

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

func TestWorker_ProcessBatch_MaxRetriesParksMessage(t *testing.T) {
	repo := newFakeRepo()
	insertMessage(repo, "msg-1", "payment.completed", DefaultMaxRetries)
	publisher := newFakePublisher()
	worker := New("test-service", repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

	if err := worker.processBatch(context.Background()); err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	msg := repo.get("msg-1")
	if msg.Processed {
		t.Fatal("a parked message must not be marked processed")
	}
	if msg.Retries != DefaultMaxRetries {
		t.Fatalf("a parked message's retries must not increment further, got %d", msg.Retries)
	}
	if publisher.callCount() != 0 {
		t.Fatalf("a parked message must never reach the publisher, got %d calls", publisher.callCount())
	}
}

func TestWorker_ProcessBatch_MixedBatch(t *testing.T) {
	repo := newFakeRepo()
	insertMessage(repo, "ok", "payment.completed", 0)
	insertMessage(repo, "retry", "payment.failed", 0)
	insertMessage(repo, "parked", "payment.completed", DefaultMaxRetries)
	publisher := newFakePublisher()
	publisher.failFor["payment.failed"] = true
	worker := New("test-service", repo, publisher, fakeTransactionManager{}, testLogger(), time.Second)

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
	if repo.get("parked").Processed || repo.get("parked").Retries != DefaultMaxRetries {
		t.Fatalf("expected 'parked' message untouched, got processed=%v retries=%d",
			repo.get("parked").Processed, repo.get("parked").Retries)
	}
	// ok + retry only — parked must never reach the publisher.
	if publisher.callCount() != 2 {
		t.Fatalf("expected 2 publish calls (ok + retry), got %d", publisher.callCount())
	}
}
