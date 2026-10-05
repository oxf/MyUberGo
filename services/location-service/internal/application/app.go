package app

import (
	"location-service/internal/application/command"
	"location-service/internal/application/query"
	"location-service/internal/common/decorator"
	"location-service/internal/domain"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	IngestPings         decorator.CommandHandler[command.IngestPings, command.IngestPingsResult]
	IngestClientPing    decorator.CommandHandler[command.IngestClientPing, command.IngestPingsResult]
	UpsertOwner         decorator.CommandHandlerNoResult[command.UpsertOwner]
	RecordRideRequested decorator.CommandHandlerNoResult[command.RecordRideRequested]
	RecordRideAccepted  decorator.CommandHandlerNoResult[command.RecordRideAccepted]
	CloseTrackingWindow decorator.CommandHandlerNoResult[command.CloseTrackingWindow]
	BuildRideSummary    decorator.CommandHandlerNoResult[command.BuildRideSummary]
}

type Queries struct {
	FindNearbyDrivers       decorator.QueryHandler[query.FindNearbyDrivers, []domain.NearbyDriver]
	GetCounterpartyPosition decorator.QueryHandler[query.GetCounterpartyPosition, query.CounterpartyPositionResult]
	ListLivePositions       decorator.QueryHandler[query.ListLivePositions, query.LivePositionsResult]
	GetRideTrack            decorator.QueryHandler[query.GetRideTrack, domain.RideSummary]
}
