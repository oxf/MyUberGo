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
	CreateDriverProfile decorator.CommandHandler[command.CreateDriverProfile, command.CreateDriverProfileResult]
	UpdateDriverProfile decorator.CommandHandlerNoResult[command.UpdateDriverProfile]
	CreateShift         decorator.CommandHandler[command.CreateShift, command.CreateShiftResult]
	UpdateShift         decorator.CommandHandlerNoResult[command.UpdateShift]
}

type Queries struct {
	GetDriverList decorator.QueryHandler[query.GetDriverList, []*domain.DriverProfile]
	GetDriverByID decorator.QueryHandler[query.GetDriverByID, *domain.DriverProfile]
	GetShiftList  decorator.QueryHandler[query.GetShiftList, []*domain.Shift]
	GetShiftByID  decorator.QueryHandler[query.GetShiftByID, *domain.Shift]
}
