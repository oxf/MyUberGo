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
	IngestPings decorator.CommandHandler[command.IngestPings, command.IngestPingsResult]
	UpsertOwner decorator.CommandHandlerNoResult[command.UpsertOwner]
}

type Queries struct {
	FindNearbyDrivers decorator.QueryHandler[query.FindNearbyDrivers, []domain.NearbyDriver]
}
