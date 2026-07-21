package app

import (
	"ride-service/internal/application/command"
	"ride-service/internal/application/query"
	"ride-service/internal/common/decorator"
	"ride-service/internal/domain"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	CreateRide      decorator.CommandHandler[command.CreateRide, command.CreateRideResult]
	MarkRideMatched decorator.CommandHandlerNoResult[command.MarkRideMatched]
}

type Queries struct {
	GetRideList decorator.QueryHandler[query.GetRideList, query.PagedResult[*domain.Ride]]
	GetRideByID decorator.QueryHandler[query.GetRideByID, *domain.Ride]
}
