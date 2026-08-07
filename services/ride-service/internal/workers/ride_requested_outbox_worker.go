package workers

import (
	"context"
	"ride-service/internal/application/services"
	"ride-service/internal/domain"
	"time"

	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/oxf/MyUber/observability/obsoutbox"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("ride-service/outbox")

const (
	defaultBatchSize      = 10
	defaultPublishTimeout = 5 * time.Second
	// defaultMaxRetries caps retries of a permanently-failing message; past the cap it's
	// parked (skipped, never processed) for manual triage instead of spamming Kafka/logs forever.
	defaultMaxRetries = 10
)

// MaxRetries exports defaultMaxRetries for cmd/main.go's outbox gauges, so the "parked"
// threshold there can never drift from what this worker actually enforces.
const MaxRetries = defaultMaxRetries

// OutboxWorker drains ride.outbox_message and publishes each row to Kafka.
// It is topic-agnostic — it publishes whatever topic each row was written with.
type OutboxWorker struct {
	repo        domain.OutboxRepository
	publisher   services.EventPublisher
	transaction services.TransactionManager
	logger      *logrus.Entry
	interval    time.Duration
	batchSize   int
}

func NewOutboxWorker(
	repo domain.OutboxRepository,
	publisher services.EventPublisher,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	interval time.Duration,
) *OutboxWorker {

	return &OutboxWorker{
		repo:        repo,
		publisher:   publisher,
		transaction: transaction,
		logger:      logger,
		interval:    interval,
		batchSize:   defaultBatchSize,
	}
}

// Run polls the outbox on a ticker until ctx is cancelled.
func (w *OutboxWorker) Run(ctx context.Context) {
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

func (w *OutboxWorker) processBatch(ctx context.Context) error {
	return w.transaction.WithinTransaction(ctx, func(txCtx context.Context) error {
		messages, err := w.repo.GetUnprocessedBatch(txCtx, w.batchSize)
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			return nil
		}

		for _, m := range messages {
			if m.Retries >= defaultMaxRetries {
				w.logger.WithField("outbox_id", m.ID).WithField("retries", m.Retries).
					Error("outbox worker: message exceeded max retries, parking (manual intervention required)")
				continue
			}

			if err := w.publishOne(txCtx, m); err != nil {
				return err
			}
		}

		return nil
	})
}

// publishOne is scoped to a single outbox row so its span ends via defer even on panic — a
// bare span.End() in processBatch's loop would never run in that case. Its error decides retry-vs-processed.
func (w *OutboxWorker) publishOne(txCtx context.Context, m *domain.OutboxMessage) (err error) {
	// Rehydrate the trace active when this row was inserted, so "publish <topic>" joins the
	// originating request's trace — the worker's own txCtx has no link back to it.
	msgCtx := obsoutbox.UnmarshalTraceContext(txCtx, m.TraceContext)
	msgCtx, span := tracer.Start(msgCtx, "publish "+m.Topic,
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

	publishCtx, cancel := context.WithTimeout(msgCtx, defaultPublishTimeout)
	publishErr = w.publisher.Publish(publishCtx, m.Topic, m.Payload)
	cancel()

	if publishErr != nil {
		w.logger.WithContext(msgCtx).WithError(publishErr).WithField("outbox_id", m.ID).Warn("outbox worker: publish failed, will retry")
		return w.repo.IncrementRetries(txCtx, m.ID)
	}

	return w.repo.MarkProcessed(txCtx, m.ID)
}
