package app

import (
	"auth-service/internal/application/command"
	"auth-service/internal/application/query"
	"auth-service/internal/common/decorator"
	"auth-service/internal/domain"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	Signup  decorator.CommandHandler[command.Signup, command.SignupResult]
	Login   decorator.CommandHandler[command.Login, command.LoginResult]
	Refresh decorator.CommandHandler[command.Refresh, command.RefreshResult]
	Logout  decorator.CommandHandlerNoResult[command.Logout]
}

type Queries struct {
	GetUserList decorator.QueryHandler[query.GetUserList, query.PagedResult[*domain.User]]
	GetUserByID decorator.QueryHandler[query.GetUserByID, *domain.User]
}
