package app

import (
	"context"

	"location-service/internal/application/command"
)

// LocationIngestor is the narrow ingest surface the WS adapter is
// constructed against, instead of the full Application (no FindNearbyDrivers etc).
type LocationIngestor interface {
	IngestDriverPings(ctx context.Context, cmd command.IngestPings) (command.IngestPingsResult, error)
	IngestClientPing(ctx context.Context, cmd command.IngestClientPing) (command.IngestPingsResult, error)
}

func (a Application) IngestDriverPings(ctx context.Context, cmd command.IngestPings) (command.IngestPingsResult, error) {
	return a.Commands.IngestPings.Handle(ctx, cmd)
}

func (a Application) IngestClientPing(ctx context.Context, cmd command.IngestClientPing) (command.IngestPingsResult, error) {
	return a.Commands.IngestClientPing.Handle(ctx, cmd)
}
