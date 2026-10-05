package workers

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"location-service/internal/domain"
	"location-service/internal/infrastructure/metrics"

	"github.com/sirupsen/logrus"
)

type fakeTracking struct {
	domain.TrackingRepository
	activeRideIDs []string
	participants  map[string]domain.Participants
}

func (f *fakeTracking) ActiveRideIDs(ctx context.Context) ([]string, error) {
	return f.activeRideIDs, nil
}

func (f *fakeTracking) Participants(ctx context.Context, rideID string) (domain.Participants, bool, error) {
	p, ok := f.participants[rideID]
	return p, ok, nil
}

type fakeTracks struct {
	domain.RideTrackRepository
	entries        map[string][]domain.TrackEntry
	archivedID     map[string]string
	setArchivedErr error
}

func (f *fakeTracks) Range(ctx context.Context, rideID string, afterID string) ([]domain.TrackEntry, error) {
	return f.entries[rideID], nil
}

func (f *fakeTracks) LastArchivedID(ctx context.Context, rideID string) (string, error) {
	return f.archivedID[rideID], nil
}

func (f *fakeTracks) SetLastArchivedID(ctx context.Context, rideID string, streamID string) error {
	if f.setArchivedErr != nil {
		return f.setArchivedErr
	}
	if f.archivedID == nil {
		f.archivedID = map[string]string{}
	}
	f.archivedID[rideID] = streamID
	return nil
}

type fakeHistory struct {
	domain.LocationHistoryRepository
	putBatches [][]domain.HistoryEntry
	putErr     error
}

func (f *fakeHistory) PutBatch(ctx context.Context, entries []domain.HistoryEntry) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.putBatches = append(f.putBatches, entries)
	return nil
}

func testWorkerLogger() *logrus.Entry {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return logrus.NewEntry(l)
}

func TestArchiveWorker_ResolvesSubjectIDFromParticipants(t *testing.T) {
	rideID := "ride-1"
	tracking := &fakeTracking{
		activeRideIDs: []string{rideID},
		participants: map[string]domain.Participants{
			rideID: {RideID: rideID, ClientID: "client-1", DriverID: "driver-1"},
		},
	}
	tracks := &fakeTracks{
		entries: map[string][]domain.TrackEntry{
			rideID: {
				{StreamID: "1-0", Subject: domain.SubjectDriver, Position: domain.Position{Coordinate: domain.Coordinate{Lat: 1, Lon: 1}}},
				{StreamID: "2-0", Subject: domain.SubjectClient, Position: domain.Position{Coordinate: domain.Coordinate{Lat: 2, Lon: 2}}},
			},
		},
	}
	history := &fakeHistory{}

	w := NewArchiveWorker(tracking, tracks, history, time.Second, 0, testWorkerLogger(), metrics.NewNoopMetricsClient())
	w.sweep(context.Background())

	if len(history.putBatches) != 1 || len(history.putBatches[0]) != 2 {
		t.Fatalf("got %+v, want one batch of 2 entries", history.putBatches)
	}
	batch := history.putBatches[0]
	if batch[0].SubjectID != "driver-1" || batch[0].SubjectType != domain.SubjectDriver {
		t.Fatalf("driver entry got %+v, want subjectID=driver-1", batch[0])
	}
	if batch[1].SubjectID != "client-1" || batch[1].SubjectType != domain.SubjectClient {
		t.Fatalf("client entry got %+v, want subjectID=client-1", batch[1])
	}

	if tracks.archivedID[rideID] != "2-0" {
		t.Fatalf("got checkpoint %q, want last entry's stream id 2-0", tracks.archivedID[rideID])
	}
}

func TestArchiveWorker_ClosedWindowIsSkippedNotError(t *testing.T) {
	tracking := &fakeTracking{activeRideIDs: []string{"ride-1"}, participants: map[string]domain.Participants{}}
	tracks := &fakeTracks{}
	history := &fakeHistory{}

	w := NewArchiveWorker(tracking, tracks, history, time.Second, 0, testWorkerLogger(), metrics.NewNoopMetricsClient())
	w.sweep(context.Background()) // must not panic; ok=false is a clean skip

	if len(history.putBatches) != 0 {
		t.Fatalf("expected no archive for a ride with no open window, got %+v", history.putBatches)
	}
}

func TestArchiveWorker_BatchCapLimitsEntriesPerTick(t *testing.T) {
	rideID := "ride-1"
	entries := make([]domain.TrackEntry, 10)
	for i := range entries {
		entries[i] = domain.TrackEntry{StreamID: string(rune('a' + i)), Subject: domain.SubjectDriver}
	}
	tracking := &fakeTracking{
		activeRideIDs: []string{rideID},
		participants:  map[string]domain.Participants{rideID: {RideID: rideID, DriverID: "driver-1"}},
	}
	tracks := &fakeTracks{entries: map[string][]domain.TrackEntry{rideID: entries}}
	history := &fakeHistory{}

	w := NewArchiveWorker(tracking, tracks, history, time.Second, 3, testWorkerLogger(), metrics.NewNoopMetricsClient())
	w.sweep(context.Background())

	if len(history.putBatches) != 1 || len(history.putBatches[0]) != 3 {
		t.Fatalf("got %+v, want exactly one batch of 3 (the configured cap)", history.putBatches)
	}
}

func TestArchiveWorker_HistoryWriteFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	rideID := "ride-1"
	tracking := &fakeTracking{
		activeRideIDs: []string{rideID},
		participants:  map[string]domain.Participants{rideID: {RideID: rideID, DriverID: "driver-1"}},
	}
	tracks := &fakeTracks{entries: map[string][]domain.TrackEntry{rideID: {{StreamID: "1-0", Subject: domain.SubjectDriver}}}}
	history := &fakeHistory{putErr: errors.New("dynamodb unavailable")}

	w := NewArchiveWorker(tracking, tracks, history, time.Second, 0, testWorkerLogger(), metrics.NewNoopMetricsClient())
	w.sweep(context.Background()) // logs and moves on, never panics or propagates

	if _, ok := tracks.archivedID[rideID]; ok {
		t.Fatalf("checkpoint must not advance when PutBatch fails, got %q", tracks.archivedID[rideID])
	}
}
