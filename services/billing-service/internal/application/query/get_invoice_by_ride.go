package query

import (
	"billing-service/internal/common/decorator"
	"billing-service/internal/domain"
	"context"

	"github.com/sirupsen/logrus"
)

// GetInvoiceByRide is used by GET /rides/{rideId}/invoice — a client
// polling for their own ride's billing outcome. Type defaults to
// ride_fare; a ride only ever has one cancellation_fee invoice too, but the
// common case (and the one e2e-test polls) is the fare invoice.
type GetInvoiceByRide struct {
	RideID string
	Type   string
}

type GetInvoiceByRideHandler struct {
	repo domain.InvoiceRepository
}

func NewGetInvoiceByRideHandler(
	repo domain.InvoiceRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetInvoiceByRide, *domain.Invoice] {

	handler := &GetInvoiceByRideHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetInvoiceByRide, *domain.Invoice](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetInvoiceByRideHandler) Handle(ctx context.Context, q GetInvoiceByRide) (*domain.Invoice, error) {
	invoiceType := q.Type
	if invoiceType == "" {
		invoiceType = domain.InvoiceTypeRideFare
	}
	return h.repo.GetByRideID(ctx, q.RideID, invoiceType)
}
