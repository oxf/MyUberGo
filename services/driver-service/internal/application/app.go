package app

import (
	"driver-service/internal/application/command"
	"driver-service/internal/application/query"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	CreateDriver         decorator.CommandHandler[command.CreateDriver, command.CreateDriverResult]
	UpdateDriver         decorator.CommandHandlerNoResult[command.UpdateDriver]
	CreateShift          decorator.CommandHandler[command.CreateShift, command.CreateShiftResult]
	UpdateShift          decorator.CommandHandlerNoResult[command.UpdateShift]
	ProcessRideAccepted  decorator.CommandHandlerNoResult[command.ProcessRideAccepted]
	ProcessRideCancelled decorator.CommandHandlerNoResult[command.ProcessRideCancelled]
	ProcessRideCompleted decorator.CommandHandlerNoResult[command.ProcessRideCompleted]
}

type Queries struct {
	GetDriverList decorator.QueryHandler[query.GetDriverList, query.PagedResult[*domain.Driver]]
	GetDriverByID decorator.QueryHandler[query.GetDriverByID, *domain.Driver]
	GetShiftList  decorator.QueryHandler[query.GetShiftList, query.PagedResult[*domain.Shift]]
	GetShiftByID  decorator.QueryHandler[query.GetShiftByID, *domain.Shift]
}
