package workers

import (
	"context"
	"ride-service/internal/application/services"
	"ride-service/internal/domain"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultBatchSize      = 10
	defaultPublishTimeout = 5 * time.Second
	// defaultMaxRetries caps how many times a permanently-failing message is
	// retried. Past the cap it's parked (skipped every tick, never marked
	// processed) rather than retried forever at a fixed cadence — it stays
	// visible via `SELECT * FROM outbox_message WHERE NOT processed AND
	// retries >= 10` for manual investigation instead of silently spamming
	// Kafka/logs indefinitely.
	defaultMaxRetries = 10
)

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

			publishCtx, cancel := context.WithTimeout(txCtx, defaultPublishTimeout)
			err := w.publisher.Publish(publishCtx, m.Topic, m.Payload)
			cancel()

			if err != nil {
				w.logger.WithError(err).WithField("outbox_id", m.ID).Warn("outbox worker: publish failed, will retry")

				if retryErr := w.repo.IncrementRetries(txCtx, m.ID); retryErr != nil {
					return retryErr
				}

				continue
			}

			if err := w.repo.MarkProcessed(txCtx, m.ID); err != nil {
				return err
			}
		}

		return nil
	})
}
