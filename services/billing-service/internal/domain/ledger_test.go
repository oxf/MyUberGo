package domain

import (
	"errors"
	"testing"
)

func TestValidateLegs_BalancedSingleCurrencyPasses(t *testing.T) {
	legs := []LedgerLeg{
		{AccountType: LedgerAccountClientReceivable, OwnerID: "client-1", Currency: "EUR", Direction: LedgerDirectionDebit, AmountMinor: 2000},
		{AccountType: LedgerAccountPlatformRevenue, Currency: "EUR", Direction: LedgerDirectionCredit, AmountMinor: 400},
		{AccountType: LedgerAccountDriverPayable, OwnerID: "driver-1", Currency: "EUR", Direction: LedgerDirectionCredit, AmountMinor: 1600},
	}
	if err := ValidateLegs("EUR", legs); err != nil {
		t.Fatalf("expected balanced legs to pass, got %v", err)
	}
}

func TestValidateLegs_UnbalancedFails(t *testing.T) {
	legs := []LedgerLeg{
		{AccountType: LedgerAccountClientReceivable, OwnerID: "client-1", Currency: "EUR", Direction: LedgerDirectionDebit, AmountMinor: 2000},
		{AccountType: LedgerAccountPlatformRevenue, Currency: "EUR", Direction: LedgerDirectionCredit, AmountMinor: 400},
		// Missing the driver_payable leg — debits (2000) != credits (400).
	}
	err := ValidateLegs("EUR", legs)
	if !errors.Is(err, ErrUnbalancedLedgerTransaction) {
		t.Fatalf("expected ErrUnbalancedLedgerTransaction, got %v", err)
	}
}

func TestValidateLegs_MixedCurrencyFails(t *testing.T) {
	legs := []LedgerLeg{
		{AccountType: LedgerAccountClientReceivable, OwnerID: "client-1", Currency: "EUR", Direction: LedgerDirectionDebit, AmountMinor: 2000},
		{AccountType: LedgerAccountPlatformRevenue, Currency: "USD", Direction: LedgerDirectionCredit, AmountMinor: 2000},
	}
	err := ValidateLegs("EUR", legs)
	if !errors.Is(err, ErrMixedCurrencyLedgerTransaction) {
		t.Fatalf("expected ErrMixedCurrencyLedgerTransaction, got %v", err)
	}
}

func TestValidateLegs_CancellationFeeHasNoDriverLeg(t *testing.T) {
	// D7: cancellation fee is 100% platform_revenue, no driver_payable leg.
	legs := []LedgerLeg{
		{AccountType: LedgerAccountClientReceivable, OwnerID: "client-1", Currency: "EUR", Direction: LedgerDirectionDebit, AmountMinor: 300},
		{AccountType: LedgerAccountPlatformRevenue, Currency: "EUR", Direction: LedgerDirectionCredit, AmountMinor: 300},
	}
	if err := ValidateLegs("EUR", legs); err != nil {
		t.Fatalf("expected balanced 2-leg cancellation fee posting to pass, got %v", err)
	}
}
