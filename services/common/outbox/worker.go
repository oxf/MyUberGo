package outbox

import (
	"context"
	"time"

	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/oxf/MyUber/observability/obsoutbox"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultBatchSize      = 10
	DefaultPublishTimeout = 5 * time.Second
	// DefaultMaxRetries caps retries for a permanently-failing message; past the cap it's
	// parked (skipped, never marked processed) instead of retried forever, staying visible for triage.
	DefaultMaxRetries = 10
	// DefaultClaimLease bounds how long a claimed row is excluded from the next tick's
	// GetUnprocessedBatch — well past DefaultPublishTimeout so a crashed worker's claim
	// self-heals without a live sweep, without also letting a slow-but-alive worker's
	// claim expire out from under it.
	DefaultClaimLease = 2 * time.Minute
)

// Worker drains a service's outbox table and publishes each row to Kafka.
// It is topic-agnostic — it publishes whatever topic each row was written with.
type Worker struct {
	repo           Repository
	publisher      Publisher
	transaction    TransactionManager
	logger         *logrus.Entry
	interval       time.Duration
	tracer         trace.Tracer
	batchSize      int
	maxRetries     int
	claimLease     time.Duration
	publishTimeout time.Duration
}

type Option func(*Worker)

func WithBatchSize(n int) Option      { return func(w *Worker) { w.batchSize = n } }
func WithMaxRetries(n int) Option     { return func(w *Worker) { w.maxRetries = n } }
func WithClaimLease(d time.Duration) Option {
	return func(w *Worker) { w.claimLease = d }
}
func WithPublishTimeout(d time.Duration) Option {
	return func(w *Worker) { w.publishTimeout = d }
}

// New builds a Worker. serviceName only names the tracer ("<serviceName>/outbox"),
// matching each service's pre-existing tracer name unchanged.
func New(
	serviceName string,
	repo Repository,
	publisher Publisher,
	transaction TransactionManager,
	logger *logrus.Entry,
	interval time.Duration,
	opts ...Option,
) *Worker {
	w := &Worker{
		repo:           repo,
		publisher:      publisher,
		transaction:    transaction,
		logger:         logger,
		interval:       interval,
		tracer:         otel.Tracer(serviceName + "/outbox"),
		batchSize:      DefaultBatchSize,
		maxRetries:     DefaultMaxRetries,
		claimLease:     DefaultClaimLease,
		publishTimeout: DefaultPublishTimeout,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// MaxRetries exposes the actually-configured threshold, so a caller's outbox
// backlog gauges can never silently drift from what this worker enforces.
func (w *Worker) MaxRetries() int { return w.maxRetries }

// Run polls the outbox on a ticker until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.processBatch(ctx); err != nil {
				w.logger.WithError(err).Error("outbox worker: batch processing failed")
			}
		}
	}
}

// processBatch claims a batch (transaction: lock+lease, released quickly) then publishes
// each claimed row with no transaction held, so a slow/failed Kafka publish never blocks
// the FOR UPDATE SKIP LOCKED row locks for longer than the claim step itself.
func (w *Worker) processBatch(ctx context.Context) error {
	messages, err := w.claimBatch(ctx)
	if err != nil {
		return err
	}
	for _, m := range messages {
		w.publishOne(ctx, m)
	}
	return nil
}

// claimBatch locks and leases the whole due batch in one short transaction. Rows already
// past w.maxRetries are left untouched (parked) rather than claimed.
func (w *Worker) claimBatch(ctx context.Context) ([]*Message, error) {
	var claimed []*Message
	err := w.transaction.WithinTransaction(ctx, func(txCtx context.Context) error {
		messages, err := w.repo.GetUnprocessedBatch(txCtx, w.batchSize)
		if err != nil {
			return err
		}

		lease := time.Now().UTC().Add(w.claimLease).Format(time.RFC3339)
		for _, m := range messages {
			if m.Retries >= w.maxRetries {
				w.logger.WithField("outbox_id", m.ID).WithField("retries", m.Retries).
					Error("outbox worker: message exceeded max retries, parking (manual intervention required)")
				continue
			}
			if err := w.repo.SetClaimedUntil(txCtx, m.ID, lease); err != nil {
				return err
			}
			claimed = append(claimed, m)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// publishOne is scoped to a single outbox row so its span ends via defer even on panic.
// Runs with no open transaction — MarkProcessed/IncrementRetries are standalone statements.
func (w *Worker) publishOne(ctx context.Context, m *Message) {
	// Rehydrate the trace active at insert (see obsoutbox) so "publish <topic>" joins the
	// originating request's trace — the worker's own ctx has no link back to it.
	msgCtx := obsoutbox.UnmarshalTraceContext(ctx, m.TraceContext)
	msgCtx, span := w.tracer.Start(msgCtx, "publish "+m.Topic,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", m.Topic),
			attribute.String("outbox.event_type", m.EventType),
		),
	)
	var publishErr error
	defer func() { obskafka.FinishSpan(span, publishErr) }()
	defer obskafka.RecoverSpan(span)

	publishCtx, cancel := context.WithTimeout(msgCtx, w.publishTimeout)
	publishErr = w.publisher.Publish(publishCtx, m.Topic, m.Payload)
	cancel()

	if publishErr != nil {
		w.logger.WithContext(msgCtx).WithError(publishErr).WithField("outbox_id", m.ID).Warn("outbox worker: publish failed, will retry")
		if err := w.repo.IncrementRetries(ctx, m.ID); err != nil {
			w.logger.WithError(err).WithField("outbox_id", m.ID).Error("outbox worker: increment retries failed")
		}
		return
	}

	if err := w.repo.MarkProcessed(ctx, m.ID); err != nil {
		w.logger.WithError(err).WithField("outbox_id", m.ID).Error("outbox worker: mark processed failed")
	}
}
