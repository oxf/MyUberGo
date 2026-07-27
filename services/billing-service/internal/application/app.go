package app

import (
	"billing-service/internal/application/command"
	"billing-service/internal/application/query"
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
)

type Application struct {
	Commands Commands
	Queries  Queries
}

type Commands struct {
	AddPaymentMethod      decorator.CommandHandler[command.AddPaymentMethod, command.AddPaymentMethodResult]
	RemovePaymentMethod   decorator.CommandHandlerNoResult[command.RemovePaymentMethod]
	CreateInvoiceFromRide decorator.CommandHandlerNoResult[command.CreateInvoiceFromRide]
}

type Queries struct {
	GetInvoice         decorator.QueryHandler[query.GetInvoice, *domain.Invoice]
	GetInvoiceByRide   decorator.QueryHandler[query.GetInvoiceByRide, *domain.Invoice]
	ListInvoices       decorator.QueryHandler[query.ListInvoices, query.PagedResult[*domain.Invoice]]
	ListPaymentMethods decorator.QueryHandler[query.ListPaymentMethods, []*domain.PaymentMethod]
	GetLedgerBalance   decorator.QueryHandler[query.GetLedgerBalance, int64]
}
