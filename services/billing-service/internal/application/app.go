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
	// FinalizeChargeSucceeded/FinalizeChargeFailed resolve a ChargeWorker
	// attempt (and, in a later pass, a payment-provider webhook) through one
	// shared code path — see finalize_charge_succeeded.go's comment.
	FinalizeChargeSucceeded decorator.CommandHandlerNoResult[command.FinalizeChargeSucceeded]
	FinalizeChargeFailed    decorator.CommandHandlerNoResult[command.FinalizeChargeFailed]
}

type Queries struct {
	GetInvoice         decorator.QueryHandler[query.GetInvoice, *domain.Invoice]
	GetInvoiceByRide   decorator.QueryHandler[query.GetInvoiceByRide, *domain.Invoice]
	ListInvoices       decorator.QueryHandler[query.ListInvoices, query.PagedResult[*domain.Invoice]]
	ListPaymentMethods decorator.QueryHandler[query.ListPaymentMethods, []*domain.PaymentMethod]
	GetLedgerBalance   decorator.QueryHandler[query.GetLedgerBalance, int64]
}
