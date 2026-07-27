package handler

import (
	"billing-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

func toPaymentMethodDto(m *domain.PaymentMethod) contracts.PaymentMethodDto {
	return contracts.PaymentMethodDto{
		Id:        m.ID,
		Brand:     m.Brand,
		Last4:     m.Last4,
		ExpMonth:  m.ExpMonth,
		ExpYear:   m.ExpYear,
		IsDefault: m.IsDefault,
		Status:    m.Status,
		CreatedAt: m.CreatedAt,
	}
}

func toInvoiceDto(inv *domain.Invoice) contracts.InvoiceDto {
	lines := make([]contracts.InvoiceLineDto, 0, len(inv.Lines))
	for _, l := range inv.Lines {
		lines = append(lines, contracts.InvoiceLineDto{
			Kind: l.Kind, AmountMinor: l.AmountMinor, Currency: l.Currency, Description: l.Description,
		})
	}
	return contracts.InvoiceDto{
		Id:           inv.ID,
		RideId:       inv.RideID,
		ClientId:     inv.ClientID,
		DriverId:     inv.DriverID,
		Type:         inv.Type,
		Status:       inv.Status,
		AmountMinor:  inv.AmountMinor,
		Currency:     inv.Currency,
		AttemptCount: inv.AttemptCount,
		Lines:        lines,
		CreatedAt:    inv.CreatedAt,
		PaidAt:       inv.PaidAt,
	}
}
