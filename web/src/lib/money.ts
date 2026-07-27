// Money is always minor units + currency (see CLAUDE.md §2 / BILLING_SPEC.md
// §2 for the repo-wide invariant). Format at the display edge only — never
// divide by 100 in component state.
export function formatMoney(amountMinor: number, currency: string): string {
  return new Intl.NumberFormat(undefined, {
    style: 'currency',
    currency: currency || 'EUR',
  }).format(amountMinor / 100);
}
