package command

import (
	"billing-service/internal/domain"
	"testing"
)

// TestBuildT1Legs_SplitSumsExactly guards BILLING_SPEC.md §2's split rule:
// truncating integer division on the platform fee, remainder to the
// driver, so fee+payable always sums exactly to the invoice amount with no
// rounding gap — the specific number (2000 minor units, 20% bps) is the
// spec's own worked example in §5.
func TestBuildT1Legs_SplitSumsExactly(t *testing.T) {
	driverID := "driver-1"
	h := &CreateInvoiceFromRideHandler{commissionBps: 2000}

	legs := h.buildT1Legs(CreateInvoiceFromRide{
		ClientID: "client-1", DriverID: &driverID, Type: domain.InvoiceTypeRideFare,
		AmountMinor: 2000, Currency: "EUR",
	})

	if err := domain.ValidateLegs("EUR", legs); err != nil {
		t.Fatalf("expected balanced legs, got %v", err)
	}

	var platformFee, driverPayable int64
	for _, l := range legs {
		switch l.AccountType {
		case domain.LedgerAccountPlatformRevenue:
			platformFee = l.AmountMinor
		case domain.LedgerAccountDriverPayable:
			driverPayable = l.AmountMinor
		}
	}
	if platformFee != 400 {
		t.Fatalf("expected platform fee 400 (20%% of 2000), got %d", platformFee)
	}
	if driverPayable != 1600 {
		t.Fatalf("expected driver payable 1600, got %d", driverPayable)
	}
	if platformFee+driverPayable != 2000 {
		t.Fatalf("fee+payable must sum exactly to amount: got %d", platformFee+driverPayable)
	}
}

// TestBuildT1Legs_CancellationFeeHasNoDriverLeg guards D7: the full fee
// credits platform_revenue with no driver_payable leg at all.
func TestBuildT1Legs_CancellationFeeHasNoDriverLeg(t *testing.T) {
	driverID := "driver-1"
	h := &CreateInvoiceFromRideHandler{commissionBps: 2000}

	legs := h.buildT1Legs(CreateInvoiceFromRide{
		ClientID: "client-1", DriverID: &driverID, Type: domain.InvoiceTypeCancellationFee,
		AmountMinor: 300, Currency: "EUR",
	})

	if err := domain.ValidateLegs("EUR", legs); err != nil {
		t.Fatalf("expected balanced legs, got %v", err)
	}
	for _, l := range legs {
		if l.AccountType == domain.LedgerAccountDriverPayable {
			t.Fatalf("cancellation fee must not post a driver_payable leg, got %+v", l)
		}
	}
}
