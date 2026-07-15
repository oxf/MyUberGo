package app

import (
	"matching-service/internal/application/command"
	"matching-service/internal/common/decorator"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	CreateDriver decorator.CommandHandlerNoResult[command.CreateDriver]
	CreateRide   decorator.CommandHandlerNoResult[command.CreateRide]
}

type Queries struct {
}
