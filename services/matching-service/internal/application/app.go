package app

import (
	"matching-service/internal/application/command"
	"matching-service/internal/application/query"
	"matching-service/internal/common/decorator"
	"matching-service/internal/domain"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	UpsertDriver    decorator.CommandHandlerNoResult[command.UpsertDriver]
	CreateRide      decorator.CommandHandlerNoResult[command.CreateRide]
	BroadcastOffers decorator.CommandHandlerNoResult[command.BroadcastOffers]
	AcceptRide      decorator.CommandHandlerNoResult[command.AcceptRide]
	CancelRide      decorator.CommandHandlerNoResult[command.CancelRide]
}

type Queries struct {
	GetDriverOffer decorator.QueryHandler[query.GetDriverOffer, *domain.DriverOffer]
}
