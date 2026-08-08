// Package kafkaconsumer provides the manual fetch/commit/retry consumer loop
// shared by every internal/consumers/*.go file across ride/driver/matching/
// billing-service. It implements the repo's deliberate at-least-once
// guarantee: a handler failure leaves the offset uncommitted and retries the
// same message in place (Kafka's committed offset is one cursor per
// partition, not a per-message ledger); a deserialization failure commits
// immediately, since a malformed payload will never parse no matter how many
// retries.
package kafkaconsumer

import (
	"context"
	"time"

	"github.com/oxf/MyUber/observability/obskafka"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultHandleTimeout            = 10 * time.Second
	DefaultRetryBackoff             = 2 * time.Second
	DefaultMaxPoisonPayloadLogBytes = 500
)

// Runner drives one Kafka consumer group over event type T.
type Runner[T any] struct {
	broker  string
	groupID string
	logger  *logrus.Entry
	tracer  trace.Tracer

	unmarshal func([]byte) (T, error)
	handle    func(ctx context.Context, event T) error
	// eventFields adds structured log fields (e.g. ride_id, driver_id) to the
	// "handle error; leaving offset uncommitted for redelivery" log line —
	// mirrors each consumer's current per-event logEntry.WithField calls.
	eventFields func(T) logrus.Fields

	handleTimeout            time.Duration
	retryBackoff             time.Duration
	maxPoisonPayloadLogBytes int
}

type Option[T any] func(*Runner[T])

func WithEventFields[T any](fn func(T) logrus.Fields) Option[T] {
	return func(r *Runner[T]) { r.eventFields = fn }
}

func WithHandleTimeout[T any](d time.Duration) Option[T] {
	return func(r *Runner[T]) { r.handleTimeout = d }
}

func WithRetryBackoff[T any](d time.Duration) Option[T] {
	return func(r *Runner[T]) { r.retryBackoff = d }
}

func WithMaxPoisonPayloadLogBytes[T any](n int) Option[T] {
	return func(r *Runner[T]) { r.maxPoisonPayloadLogBytes = n }
}

// New builds a Runner. groupID is used both as the Kafka consumer group and
// as the tracer name prefix ("<groupID>/consumer"), matching every existing
// consumer file's convention of GroupID == serviceName.
func New[T any](
	broker, groupID string,
	logger *logrus.Entry,
	unmarshal func([]byte) (T, error),
	handle func(context.Context, T) error,
	opts ...Option[T],
) *Runner[T] {
	r := &Runner[T]{
		broker:                   broker,
		groupID:                  groupID,
		logger:                   logger,
		tracer:                   otel.Tracer(groupID + "/consumer"),
		unmarshal:                unmarshal,
		handle:                   handle,
		handleTimeout:            DefaultHandleTimeout,
		retryBackoff:             DefaultRetryBackoff,
		maxPoisonPayloadLogBytes: DefaultMaxPoisonPayloadLogBytes,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run manually fetches/commits offsets and retries a failed message in
// place, since Kafka's committed offset is one cursor per partition — the
// caller's Handle must itself be safely redeliverable (a guarded/idempotent
// transition).
func (r *Runner[T]) Run(ctx context.Context, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{r.broker},
		Topic:   topic,
		GroupID: r.groupID,
	})
	defer reader.Close()

	r.logger.WithField("topic", topic).Info("consumer started")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.WithError(err).Error("consumer error")
			continue
		}

		for {
			commit, _ := r.handleOne(ctx, topic, msg)
			if commit {
				if err := reader.CommitMessages(ctx, msg); err != nil {
					r.logger.WithError(err).Error("commit offset failed")
				}
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.retryBackoff):
			}
		}
	}
}

// handleOne is scoped to a single message so its span/timeout-cancel run via defer even on panic.
// The span starts before deserialization so a poison message still produces one and is committed.
func (r *Runner[T]) handleOne(ctx context.Context, topic string, msg kafka.Message) (commit bool, err error) {
	msgCtx := obskafka.Extract(ctx, msg.Headers)
	msgCtx, span := obskafka.StartConsumerSpan(msgCtx, r.tracer, topic, r.groupID, msg)
	defer func() { obskafka.FinishSpan(span, err) }()
	defer obskafka.RecoverSpan(span)

	event, err := r.unmarshal(msg.Value)
	if err != nil {
		span.SetAttributes(attribute.Bool("messaging.kafka.poison_message", true))
		r.logger.WithContext(msgCtx).WithError(err).
			WithField("raw_payload", obskafka.TruncateForLog(msg.Value, r.maxPoisonPayloadLogBytes)).
			Error("failed to deserialize event; committing to skip poison message")
		return true, err
	}

	logEntry := r.logger.WithContext(msgCtx)
	if r.eventFields != nil {
		logEntry = logEntry.WithFields(r.eventFields(event))
	}

	handleCtx, cancel := context.WithTimeout(msgCtx, r.handleTimeout)
	defer cancel()
	if err = r.handle(handleCtx, event); err != nil {
		logEntry.WithError(err).Error("handle error; leaving offset uncommitted for redelivery")
		return false, err
	}
	return true, nil
}
