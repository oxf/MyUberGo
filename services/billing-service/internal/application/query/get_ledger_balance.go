package query

import (
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"

	"github.com/sirupsen/logrus"
)

// GetLedgerBalance is the cheapest possible regression check for the
// ledger invariants — e.g. asserting client_receivable == fare right after
// T1, or that a client with one EUR and one USD ride has two distinct
// balances (BILLING_SPEC.md §10).
type GetLedgerBalance struct {
	AccountType string
	OwnerID     string
	Currency    string
}

type GetLedgerBalanceHandler struct {
	repo domain.LedgerRepository
}

func NewGetLedgerBalanceHandler(
	repo domain.LedgerRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetLedgerBalance, int64] {

	handler := &GetLedgerBalanceHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetLedgerBalance, int64](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetLedgerBalanceHandler) Handle(ctx context.Context, q GetLedgerBalance) (int64, error) {
	return h.repo.GetBalance(ctx, q.AccountType, q.OwnerID, q.Currency)
}
