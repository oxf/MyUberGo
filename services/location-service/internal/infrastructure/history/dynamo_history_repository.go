// Package history archives raw pings to the long-retention, eventually
// consistent history store — DynamoDB Local in this repo (LOCATION_SPEC.md
// §6.2). This tier is an audit/verification input, never a system of
// record: actual distance is never authoritative for money (§2.4), so this
// store may be lossy under load and callers must never let ingest latency
// depend on it being healthy.
package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	"location-service/internal/domain"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/sirupsen/logrus"
)

const (
	// TableName: PK=subject_id, SK=ts_ms, GSI on ride_id — the shape
	// LOCATION_SPEC.md §6.2 specifies.
	TableName     = "location_ping_history"
	rideIndexName = "ride_id-ts_ms-index"

	// batchWriteLimit is DynamoDB's hard cap on items per BatchWriteItem call.
	batchWriteLimit = 25
)

// NewClient builds a DynamoDB client. An empty endpoint uses the SDK's
// normal region/credential chain (real AWS); a non-empty one overrides the
// resolver for DynamoDB Local, which needs *some* static credentials to
// sign requests even though it never validates them.
func NewClient(ctx context.Context, endpoint string) (*dynamodb.Client, error) {
	opts := []func(*config.LoadOptions) error{config.WithRegion("us-east-1")}
	if endpoint != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", ""),
		))
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

// Repository implements domain.LocationHistoryRepository against DynamoDB.
type Repository struct {
	client  *dynamodb.Client
	ttlDays int
	logger  *logrus.Entry
}

func NewRepository(client *dynamodb.Client, ttlDays int, logger *logrus.Entry) *Repository {
	if client == nil {
		panic("nil client")
	}
	return &Repository{client: client, ttlDays: ttlDays, logger: logger}
}

// EnsureTable creates the table (and its GSI) if it doesn't already exist —
// DynamoDB Local does not auto-create tables the way the migrate one-shot
// does for Postgres. Idempotent: a ResourceInUseException means the table
// already exists, which is success, not an error.
func (r *Repository) EnsureTable(ctx context.Context) error {
	_, err := r.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(TableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("subject_id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("ts_ms"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("ride_id"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("subject_id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("ts_ms"), KeyType: types.KeyTypeRange},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(rideIndexName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("ride_id"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("ts_ms"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		var inUse *types.ResourceInUseException
		if errors.As(err, &inUse) {
			return nil
		}
		return err
	}

	waiter := dynamodb.NewTableExistsWaiter(r.client)
	if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(TableName)}, 30*time.Second); err != nil {
		return err
	}

	// Best-effort: DynamoDB Local accepts UpdateTimeToLive but (unlike real
	// AWS) does not actually expire items on it. Never fails boot over this.
	if _, err := r.client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(TableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expires_at"),
			Enabled:       aws.Bool(true),
		},
	}); err != nil && r.logger != nil {
		r.logger.WithError(err).Warn("dynamodb: failed to enable TTL (non-fatal, best-effort on DynamoDB Local)")
	}

	return nil
}

type pingItem struct {
	SubjectID  string  `dynamodbav:"subject_id"`
	TsMs       int64   `dynamodbav:"ts_ms"`
	RideID     string  `dynamodbav:"ride_id"`
	Subject    string  `dynamodbav:"subject"`
	Lat        float64 `dynamodbav:"lat"`
	Lon        float64 `dynamodbav:"lon"`
	AccuracyM  float64 `dynamodbav:"accuracy_m"`
	HeadingDeg float64 `dynamodbav:"heading_deg"`
	SpeedMps   float64 `dynamodbav:"speed_mps"`
	ExpiresAt  int64   `dynamodbav:"expires_at"`
}

// PutBatch archives entries in chunks of at most 25 (DynamoDB's
// BatchWriteItem limit), called by ArchiveWorker on a ticker — never from
// the synchronous ingest path.
func (r *Repository) PutBatch(ctx context.Context, entries []domain.HistoryEntry) error {
	for start := 0; start < len(entries); start += batchWriteLimit {
		end := start + batchWriteLimit
		if end > len(entries) {
			end = len(entries)
		}
		if err := r.putChunk(ctx, entries[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) putChunk(ctx context.Context, chunk []domain.HistoryEntry) error {
	writes := make([]types.WriteRequest, 0, len(chunk))
	for _, e := range chunk {
		item, err := attributevalue.MarshalMap(pingItem{
			SubjectID:  e.SubjectID,
			TsMs:       e.Position.ServerTs.UnixMilli(),
			RideID:     e.RideID,
			Subject:    string(e.SubjectType),
			Lat:        e.Position.Coordinate.Lat,
			Lon:        e.Position.Coordinate.Lon,
			AccuracyM:  e.Position.AccuracyM,
			HeadingDeg: e.Position.HeadingDeg,
			SpeedMps:   e.Position.SpeedMps,
			ExpiresAt:  e.Position.ServerTs.AddDate(0, 0, r.ttlDays).Unix(),
		})
		if err != nil {
			return err
		}
		writes = append(writes, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}

	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{TableName: writes}}

	// Retry UnprocessedItems once — DynamoDB's own throughput throttling can
	// leave a partial batch unprocessed even on an otherwise-successful call.
	for attempt := 0; attempt < 2; attempt++ {
		out, err := r.client.BatchWriteItem(ctx, input)
		if err != nil {
			return err
		}
		if len(out.UnprocessedItems) == 0 {
			return nil
		}
		input.RequestItems = out.UnprocessedItems
	}
	return fmt.Errorf("dynamodb: %d items still unprocessed after retry", len(input.RequestItems[TableName]))
}
