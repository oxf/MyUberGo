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
	MarkRideBilled  decorator.CommandHandlerNoResult[command.MarkRideBilled]
	CancelRide      decorator.CommandHandler[command.CancelRide, command.CancelRideResult]
	StartRide       decorator.CommandHandler[command.StartRide, command.StartRideResult]
	CompleteRide    decorator.CommandHandler[command.CompleteRide, command.CompleteRideResult]
}

type Queries struct {
	GetRideList decorator.QueryHandler[query.GetRideList, query.PagedResult[*domain.Ride]]
	GetRideByID decorator.QueryHandler[query.GetRideByID, *domain.Ride]
}
