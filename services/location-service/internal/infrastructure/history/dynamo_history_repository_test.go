package history

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"location-service/internal/domain"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// No dedicated testcontainers DynamoDB module is vendored in this repo
// (grep confirms no modules/dynamodb in any go.sum) — a plain
// GenericContainer against amazon/dynamodb-local is the lightest correct
// alternative, mirroring what a module wrapper would do internally.
var testClient *dynamodb.Client

var seedSeq atomic.Int64

func nextSubjectID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, seedSeq.Add(1))
}

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "amazon/dynamodb-local:2.5.2",
		ExposedPorts: []string{"8000/tcp"},
		Cmd:          []string{"-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"},
		WaitingFor:   wait.ForListeningPort("8000/tcp"),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("start dynamodb-local container: %v", err)
	}
	defer func() {
		if err := ctr.Terminate(ctx); err != nil {
			log.Printf("terminate dynamodb-local container: %v", err)
		}
	}()

	host, err := ctr.Host(ctx)
	if err != nil {
		log.Fatalf("dynamodb-local host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "8000/tcp")
	if err != nil {
		log.Fatalf("dynamodb-local mapped port: %v", err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	client, err := NewClient(ctx, endpoint)
	if err != nil {
		log.Fatalf("new dynamodb client: %v", err)
	}
	testClient = client

	repo := NewRepository(testClient, 30, nil)
	if err := repo.EnsureTable(ctx); err != nil {
		log.Fatalf("ensure table: %v", err)
	}

	return m.Run()
}

func TestEnsureTable_IdempotentOnSecondCall(t *testing.T) {
	repo := NewRepository(testClient, 30, nil)
	if err := repo.EnsureTable(context.Background()); err != nil {
		t.Fatalf("second EnsureTable call: %v", err)
	}
}

func TestPutBatch_WritesAndIsQueryableByRideIndex(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(testClient, 30, nil)

	rideID := nextSubjectID("ride")
	subjectID := nextSubjectID("driver")
	base := time.Now().UTC()

	entries := make([]domain.HistoryEntry, 0, 3)
	for i := 0; i < 3; i++ {
		entries = append(entries, domain.HistoryEntry{
			SubjectID:   subjectID,
			SubjectType: domain.SubjectDriver,
			RideID:      rideID,
			Position: domain.Position{
				Coordinate: domain.Coordinate{Lat: 34.700 + float64(i)*0.001, Lon: 33.000},
				DeviceTs:   base.Add(time.Duration(i) * time.Second),
				ServerTs:   base.Add(time.Duration(i) * time.Second),
			},
		})
	}

	if err := repo.PutBatch(ctx, entries); err != nil {
		t.Fatalf("PutBatch: %v", err)
	}

	out, err := testClient.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(TableName),
		IndexName:              aws.String(rideIndexName),
		KeyConditionExpression: aws.String("ride_id = :r"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":r": &types.AttributeValueMemberS{Value: rideID},
		},
	})
	if err != nil {
		t.Fatalf("query by ride index: %v", err)
	}
	if len(out.Items) != 3 {
		t.Fatalf("got %d items for ride %s, want 3", len(out.Items), rideID)
	}
}

func TestPutBatch_MoreThan25ItemsChunks(t *testing.T) {
	ctx := context.Background()
	repo := NewRepository(testClient, 30, nil)

	rideID := nextSubjectID("ride")
	subjectID := nextSubjectID("driver")
	base := time.Now().UTC()

	entries := make([]domain.HistoryEntry, 0, 60)
	for i := 0; i < 60; i++ {
		entries = append(entries, domain.HistoryEntry{
			SubjectID:   subjectID,
			SubjectType: domain.SubjectDriver,
			RideID:      rideID,
			Position: domain.Position{
				Coordinate: domain.Coordinate{Lat: 34.7, Lon: 33.0},
				DeviceTs:   base.Add(time.Duration(i) * time.Millisecond),
				ServerTs:   base.Add(time.Duration(i) * time.Millisecond),
			},
		})
	}

	if err := repo.PutBatch(ctx, entries); err != nil {
		t.Fatalf("PutBatch (60 items, >25 chunk limit): %v", err)
	}
}
