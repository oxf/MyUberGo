package workers

import (
	"billing-service/internal/application/services"
	"billing-service/internal/domain"
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	defaultBatchSize      = 10
	defaultPublishTimeout = 5 * time.Second
)

// OutboxWorker drains billing.outbox_message and publishes each row to
// Kafka. It is topic-agnostic — it publishes whatever topic each row was
// written with (payment.completed, payment.failed).
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
