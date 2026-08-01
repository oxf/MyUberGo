# billing-service — Phase 1 Spec

Hand-off spec for Claude Code. Follows the conventions in `CLAUDE.md` / `PLAN.md`.
Target: a Stage-2 (CQRS/DDD) `billing-service` that turns completed rides into invoices, collects
them off-session through a pluggable payment provider (stub in v1, Stripe later), and records every
money movement in a double-entry ledger.

---

## 0. Scope

**In scope (v1)**
- Money represented as integer minor units + ISO-4217 currency, repo-wide.
- Multi-currency support: EUR and USD.
- `billing` Postgres schema: customers, payment methods, invoices, payments, double-entry ledger.
- `ride.completed` and `ride.cancelled` consumers → invoice creation.
- `PaymentProvider` port + in-process stub adapter (deterministic, forced outcomes for tests).
- `ChargeWorker`: sweeps open invoices, attempts collection, retries with backoff, gives up as
  `uncollectible`.
- `payment.completed` / `payment.failed` published via the existing transactional-outbox pattern.
- e2e-test coverage of the happy path and the decline path.

**Explicitly out of scope (v1)** — see §9 for why and what stays additive.
- Real Stripe adapter, webhooks, `psp_event` inbox (✅ built in a later pass, 2026-08-01 — see §9), reconciliation, PSP-fee capture (still deferred).
- Driver payouts / Stripe Connect (`driver_payable` **is** modelled — see §5).
- Client wallet, credits, promos, refunds, disputes.
- Authorize-hold at match / capture at completion.
- Pre-ride payment-method validation and outstanding-balance blocking.
- FX conversion between currencies.
- Receipt rendering (invoice + lines are the receipt data; Notification service will format it).

---

## 1. Decisions locked

| # | Decision | Rationale |
|---|---|---|
| D1 | Charge **once, at ride completion**. No pre-auth hold. | Hold+capture doubles the state machine. It catches dead cards before the ride, which matters in production but not here. Additive later. |
| D2 | **No payment-method guard** at ride request. | Requires the first sync service-to-service HTTP call in the repo; deserves its own design pass. Invoices simply go `uncollectible`. |
| D3 | Bill the **stored `estimated_price`**, not a recomputed final fare. | `actual_distance_km` exists but nothing produces a final-fare rule yet. Recomputation is a ride-service concern, not billing's. Revisit when time-of-day tariffs land. |
| D4 | Commission is a **configurable integer constant in basis points**, env `PLATFORM_COMMISSION_BPS` (default `2000` = 20%). | Integer bps avoids float. A `commission_rule` table is premature until rates vary by city/tier. |
| D5 | Payment provider is an **in-process stub** behind a port, not a separate container. | Simpler; the port is the real boundary. Swapping in Stripe is an adapter + config change. |
| D6 | Stub returns **success synchronously**, but the domain status enum and `payment` table model the async states from day one. | Keeps the webhook path additive instead of a refactor. Same precedent as publishing `ride.started` with no consumer. |
| D7 | Cancellation fee goes **100% to `platform_revenue`**, no driver split. | Simplest defensible policy. Real Uber splits it; changing this later is a change to one ledger-posting function, not the schema. |
| D8 | Currency is carried on the **tariff** and copied onto the ride. | Single source; a ride's currency can never drift from its price. |

---

## 2. Money representation (repo-wide invariant)

Applies to **every** monetary value in every service, contract, and table.

- Amount: `BIGINT` in Postgres, `int64` in Go. **Minor units** (cents).
- Currency: `CHAR(3)` in Postgres, `string` in Go. ISO-4217 uppercase. Allowed: `EUR`, `USD`.
- **A money value is always a pair.** Never store or pass an amount without its currency.
- Assumption documented: both supported currencies have exponent 2. Adding a 0-decimal currency
  (JPY) would require a per-currency exponent lookup. Not built.

**Rounding rule.** Rates are integers (minor units); distance and duration are genuinely
fractional. Therefore: do the fractional multiplication in `float64`, round **once**, at the end,
to `int64`. Never persist or pass an intermediate float.

```
fare_minor = base_fare_minor
           + int64(math.Round(float64(price_per_km_minor) * distance_km))
           + int64(math.Round(float64(price_per_min_minor) * duration_min))
```

**Split rule (must sum exactly).** Truncating integer division on the fee, remainder to the driver:

```
platform_fee_minor = amount_minor * bps / 10000      // integer division, truncates
driver_payable_minor = amount_minor - platform_fee_minor
```

This guarantees `platform_fee + driver_payable == amount` with no rounding gap. Do **not** compute
both sides independently.

---

## 3. Step 1 — Money types + currency across the repo

No migration tool exists. `init.sql` changes require `docker-compose down -v` (same caveat as the
`OnRide` CHECK-constraint change). Call this out in `PLAN.md`.

### 3.1 `services/shared/migrations/init.sql`

| Table | Change |
|---|---|
| `ride.tariff` | `base_fare`, `price_per_km`, `price_per_min` → `*_minor BIGINT NOT NULL`; add `currency CHAR(3) NOT NULL DEFAULT 'EUR'`. Update the four seed rows to minor units. |
| `ride.ride` | `estimated_price` → `estimated_price_minor BIGINT NOT NULL`; add `currency CHAR(3) NOT NULL`. |
| `driver.shift` | `total_earnings` → `total_earnings_minor BIGINT NOT NULL DEFAULT 0`; add `currency CHAR(3) NOT NULL DEFAULT 'EUR'`. |

Leave `estimated_distance_km` / `actual_distance_km` as `DOUBLE PRECISION` — distance is genuinely
fractional and is not money.

### 3.2 `services/contracts`

- `http`: every price/earnings field → `amountMinor int64` + `currency string`. Affects ride
  request/response DTOs, shift DTOs, and anything the `web/` dashboard reads.
- `kafka`:
  - `RideCompletedEvent` gains `AmountMinor int64`, `Currency string`, `DriverID string`.
  - `RideCancelledEvent` gains `FeeMinor int64`, `Currency string` (`DriverID` already present,
    nullable — it stays nullable for pre-match cancellations).
  - Both must carry a stable `RideID` — already true.
- Contracts stay dumb structs. Do not introduce a `Money` type into `contracts`; JSON shape must
  stay obvious. A small `Money` value object inside each service's `domain` package is fine.

### 3.3 Services

- **ride-service**: fare calculation moves to integer minor units per §2. `CancellationFeeCalculator`
  port signature returns `(int64, string, error)` — amount + currency. `StubCalculator` still
  returns 0 (see §8 for what that means). Populate the new event fields when publishing
  `ride.completed` / `ride.cancelled` through the outbox.
- **driver-service**: shift earnings accumulation switches to `int64`.
- **web/**: TypeScript DTOs updated; format minor units for display at the edge only.
- **e2e-test**: assertions updated to minor units.
- **Bruno**: request bodies and any hardcoded prices updated.

**Definition of done:** `go build ./... && go vet ./... && go test ./...` clean on all 4 services;
`tsc --noEmit` clean in `web/`; full e2e suite green against a fresh `docker-compose up --build`.

Do this as its own commit before any billing work starts.

---

## 4. Step 2 — `billing` schema

New schema `billing` in `services/shared/migrations/init.sql`. All money columns follow §2.

### 4.1 Provider identity

**`billing.customer`** — one provider-side customer per client per provider.
`id`, `client_id` (FK `auth.client(id)`), `provider`, `provider_customer_id`, `created_at`.
Unique `(client_id, provider)`.

**`billing.payment_method`**
`id`, `client_id` (FK `auth.client(id)`), `provider`, `provider_payment_method_id`, `brand`,
`last4`, `exp_month`, `exp_year`, `is_default BOOLEAN`, `status` (`active` | `expired` | `removed`),
`created_at`, `updated_at`.
- Partial unique index on `client_id WHERE is_default AND status = 'active'` — exactly one default.
- **Never store a PAN, CVC, or full expiry-plus-number.** `brand`/`last4` are display metadata only.

### 4.2 What is owed

**`billing.invoice`**
`id`, `ride_id`, `client_id`, `driver_id` (nullable — null for pre-match cancellations),
`type` (`ride_fare` | `cancellation_fee`), `status` (`open` | `paid` | `uncollectible` | `void`),
`amount_minor`, `currency`, `attempt_count INT NOT NULL DEFAULT 0`,
`next_attempt_at TIMESTAMPTZ`, `created_at`, `updated_at`, `paid_at`.
- **Unique `(ride_id, type)`** — the idempotency guard against redelivered Kafka events. This is the
  single most important constraint in the schema.
- Index on `(status, next_attempt_at)` for the `ChargeWorker` sweep.
- No `draft` status: it exists for multi-line accumulation that doesn't happen here.

**`billing.invoice_line`**
`id`, `invoice_id`, `kind` (`base_fare` | `distance` | `time` | `cancellation_fee` |
`platform_fee`), `amount_minor`, `currency`, `description`, `created_at`.
- v1 writes a single line. The table exists so receipts and tax are additive later.

### 4.3 Collection attempts

**`billing.payment`** — one row per **attempt**, never mutated in place except for status.
`id`, `invoice_id`, `attempt_no INT`, `provider`, `provider_payment_intent_id` (nullable),
`payment_method_id`, `amount_minor`, `currency`,
`status` (`pending` | `processing` | `succeeded` | `failed`),
`failure_code`, `failure_message`, `idempotency_key`, `created_at`, `updated_at`.
- Unique `(invoice_id, attempt_no)`.
- Unique `idempotency_key`. Format: `invoice:{invoice_id}:attempt:{attempt_no}` — deterministic, so
  a crashed-and-retried attempt reuses the same key and cannot double-charge.
- `processing` is unreachable with the v1 stub (D6). Handle it anyway: leave the row alone and let
  the invoice's `next_attempt_at` remain unset until a future webhook/poller resolves it.

### 4.4 The ledger

**`billing.ledger_account`**
`id`, `type` (`client_receivable` | `driver_payable` | `platform_revenue` | `psp_clearing` |
`psp_fees` | `bad_debt`), `owner_id UUID NULL`, `currency CHAR(3)`, `created_at`.
- **Unique `(type, owner_id, currency)`.** `owner_id` is null for platform-level accounts.
- **Accounts are per-currency.** A client with EUR and USD rides has two `client_receivable`
  accounts. This is not optional — mixing currencies in one account makes the balance meaningless.
- Created lazily on first use (get-or-create inside the posting transaction).

**`billing.ledger_transaction`**
`id`, `type` (`invoice_opened` | `payment_succeeded` | `invoice_uncollectible`),
`ref_type`, `ref_id`, `currency`, `created_at`.

**`billing.ledger_entry`** — **append-only. Never UPDATE, never DELETE.**
`id`, `transaction_id` (FK), `account_id` (FK), `direction` (`debit` | `credit`),
`amount_minor BIGINT` (always positive), `currency`, `created_at`.
- Index on `(account_id, created_at)`.
- Corrections are new compensating transactions, never edits.

**Invariants** — enforce in the domain layer, and add a test:
1. Every transaction's debits equal its credits.
2. A transaction never mixes currencies (v1 has no FX).
3. Account balance = `SUM(credits) − SUM(debits)` scoped to one currency. **Never a stored column.**
4. Never `SUM` across currencies in any query.

### 4.5 Plumbing

**`billing.outbox_message`** — copy `ride.outbox_message` verbatim, plus the worker from
driver-service (`OutboxWorker`, the topic-agnostic one with `FOR UPDATE SKIP LOCKED`).

---

## 5. Ledger postings

€20.00 ride, 20% commission. All amounts in cents. Note `driver_payable` is posted in v1 even
though payouts are out of scope — skipping it would mean backfilling every historical entry later.

**T1 — `invoice_opened`** (in the same DB transaction as the invoice INSERT)

| Account | Dr | Cr |
|---|---|---|
| `client_receivable:{client}:EUR` | 2000 | |
| `platform_revenue:EUR` | | 400 |
| `driver_payable:{driver}:EUR` | | 1600 |

**T2 — `payment_succeeded`** (same DB transaction as the invoice → `paid` update)

| Account | Dr | Cr |
|---|---|---|
| `psp_clearing:EUR` | 2000 | |
| `client_receivable:{client}:EUR` | | 2000 |

**T3 — `invoice_uncollectible`** (instead of T2, after retries are exhausted)

| Account | Dr | Cr |
|---|---|---|
| `bad_debt:EUR` | 2000 | |
| `client_receivable:{client}:EUR` | | 2000 |

Note `driver_payable` stays at 1600 in the T3 case: **the driver is owed money the platform never
collected.** That divergence is the entire reason for double-entry, and a single wallet-balance
column cannot express it.

For a **cancellation fee** (D7), T1 has no `driver_payable` leg — the full amount credits
`platform_revenue`.

`psp_fees` is declared but unused in v1 — the real fee is only known from a Stripe balance
transaction.

---

## 6. Step 3 — Service skeleton

Stage 2 from day one. Copy `ride-service`'s layering — do not start at Stage 1.

```
services/billing-service/
  cmd/main.go                        # thin composition root
  internal/domain/                   # Invoice, Payment, LedgerTransaction, Money; repository ports
  internal/application/
    command/                         # CreateInvoiceFromRide, AttemptPayment, MarkUncollectible,
                                     # AddPaymentMethod
    query/                           # GetInvoice, ListInvoices, GetPaymentMethods, GetLedgerBalance
    services/                        # PaymentProvider, EventPublisher ports
  internal/persistence/              # Postgres repos, tx-aware via Executor(ctx, db)
  internal/infrastructure/
    payment/stub/                    # StubProvider
    kafka/                           # Publisher
  internal/interfaces/http/          # handlers, decorator-wrapped (logging + metrics)
  internal/consumers/                # RideCompletedConsumer, RideCancelledConsumer
  internal/workers/                  # OutboxWorker, ChargeWorker
  Dockerfile
```

Required infrastructure, matching the other Stage-2 services:
- `TransactionManager` with context propagation (copy ride-service's `persistence` pattern).
- Graceful shutdown via `shutdown.Manager`, including `OnStop` to cancel `ChargeWorker`'s context.
- Health checks, logging/metrics decorators.
- Port `:8005` (matches README's target).
- `docker-compose.yml` entry.
- `gateway/kong.yml`: `/api/billing/*` → billing-service, protected route with
  `inject_user_headers` (billing reads `X-Client-Id`).

**Dockerfile trap:** use `COPY services/billing-service .`, not `COPY services/billing-service/cmd ./cmd`.
The `cmd`-only form is what broke auth-service's image build when `internal/` packages were added.

---

## 7. Step 4 — Payment methods

`POST /payment-methods` — body: `{ "providerPaymentMethodId": "pm_stub_ok", "brand": "visa",
"last4": "4242", "expMonth": 12, "expYear": 2030, "setDefault": true }`. Client from `X-Client-Id`.
Creates the `billing.customer` row on first call (get-or-create).

`GET /payment-methods` — the caller's own methods. Never returns provider secrets.

`DELETE /payment-methods/{id}` — soft: `status = 'removed'`. 409 if it is the only active default
and the client has open invoices.

---

## 8. Steps 5–9 — Ordered implementation

**Step 5 — `ride.completed` consumer → invoice + ledger**
- `GroupID: "billing-service"`. Same reader-loop shape as the other services' consumers.
- `CreateInvoiceFromRide` command, inside `WithinTransaction`: insert invoice (`open`,
  `next_attempt_at = now()`), insert invoice line, post T1.
- Idempotency: rely on the unique `(ride_id, type)` violation → treat as a no-op success, ack the
  message. Do not pre-check with a SELECT (race).
- **Done when:** e2e asserts an `open` invoice with the right amount/currency, and that
  `client_receivable` balance equals the fare.

**Step 6 — Stub provider + `ChargeWorker`**
- Port: `Charge(ctx, ChargeRequest) (ChargeResult, error)`. Define **your own** result type — do not
  mirror Stripe's response shape. `ChargeResult{Status, ProviderIntentID, FailureCode, FailureMessage}`
  with `Status ∈ {Succeeded, Processing, RequiresAction, Failed}`.
- `StubProvider` picks its outcome deterministically from the payment-method token prefix:
  `pm_stub_ok` → Succeeded, `pm_stub_decline` → Failed(`card_declined`),
  `pm_stub_insufficient` → Failed(`insufficient_funds`). Honours the idempotency key: same key →
  same cached result, no second "charge".
- `ChargeWorker`: ticker + `select`, same shape as the outbox workers. Each tick selects invoices
  `WHERE status='open' AND next_attempt_at <= now()` with `FOR UPDATE SKIP LOCKED`, then per invoice:
  insert `payment` row (`pending`) → call provider → on `Succeeded`, in one transaction: payment →
  `succeeded`, invoice → `paid`, post T2, insert `payment.completed` outbox row.
- **Done when:** e2e sees an invoice go `open` → `paid` and `psp_clearing` balance the fare.

**Step 7 — Failure path**
- On `Failed`: payment → `failed` with the code, invoice `attempt_count++`,
  `next_attempt_at = now() + backoff(attempt_count)` (e.g. 1m / 5m / 30m — compressed for a pet
  project; production would be days).
- After `MAX_PAYMENT_ATTEMPTS` (env, default 3): invoice → `uncollectible`, post T3, publish
  `payment.failed`.
- **Done when:** e2e drives a `pm_stub_decline` client through 3 attempts to `uncollectible` and
  asserts `bad_debt` balance.

**Step 8 — `ride.cancelled` with fee**
- Same pipeline, `type = 'cancellation_fee'`, T1 without the driver leg (D7).
- **Skip invoice creation entirely when `feeMinor == 0`.** ride-service's `StubCalculator` returns 0
  today, so nothing will flow through this path until a real fee rule exists — that is expected, not
  a bug. Consider giving the stub a non-zero flat fee (e.g. 300 minor units when a driver was
  assigned) purely so the path is exercisable.

**Step 9 — e2e-test**
- `client_actor.go`: attach `pm_stub_ok` at signup; after `ride.complete`, poll for invoice `paid`
  (async over Kafka — reuse the `verifyOnRide` retry idiom). New ops: `billing.paymentmethod.add`,
  `billing.invoice.get`, `billing.invoice.paid`.
- A dedicated declining client using `pm_stub_decline` asserting `uncollectible`.
- A ledger-balance assertion op. Cheapest possible regression test for the invariants.
- Bruno: a `Billing Service` folder mirroring the endpoints.

---

## 9. Deferred, and why it stays additive

| Deferred | Why it's safe to defer | What keeps it additive |
|---|---|---|
| ~~Stripe adapter~~ ✅ done (2026-08-01) | Stub proved the pipeline first | Delivered as `StripeProvider` (`internal/infrastructure/payment/stripe`), implementing the same `PaymentProvider`/`CustomerVault` ports — no domain refactor needed, exactly as planned |
| ~~Webhooks + `psp_event` inbox~~ ✅ done (2026-08-01) | No real async provider was wired up yet | Delivered as `WebhookHandler` + `billing.psp_event`; `payment.status`'s pre-existing `processing` state and `provider_payment_intent_id` column were exactly what this needed, no schema change |
| Poller for stuck `processing` payments | Now reachable (real async provider exists), but not yet built | Same as above — this is the next open item |
| PSP fee capture | Fee is only known from a Stripe balance transaction | `psp_fees` account type already declared |
| Reconciliation runs | Needs real provider data to reconcile against | The ledger is the reconciliation target and exists from day one |
| Driver payouts / Connect | Own phase | `driver_payable` is posted from day one — this is the expensive one to retrofit, so it is **not** deferred |
| Outstanding-balance blocking of new rides | Needs sync service-to-service HTTP | Becomes a query over `client_receivable`, not a new table |
| Refunds, disputes, wallet, promos, FX | Not needed to learn the core pattern | New tables/accounts, no changes to existing ones |

---

## 10. Risks and failure modes

- **Fresh-volume requirement.** Both §3 and §4 change `init.sql`, and there is still no migration
  tool. Every developer needs `docker-compose down -v`. This is now the third schema change with
  this caveat — worth considering `golang-migrate` as a separate task.
- **Float→int conversion is silent.** A missed conversion compiles fine and produces a 100× error.
  Rename columns and fields (`price` → `priceMinor`) rather than changing types in place, so the
  compiler finds every call site.
- **At-least-once delivery.** Both consumers must be idempotent. The unique `(ride_id, type)`
  constraint is the only thing standing between a redelivered event and a double charge.
- **Currency mixing.** The easiest bug to introduce and the hardest to notice: a `SUM(amount_minor)`
  without a `GROUP BY currency`. Add a test that a client with one EUR and one USD ride has two
  distinct receivable balances.
- **Ledger drift.** Any code path that writes an invoice without posting a matching transaction
  silently breaks the books. Keep posting inside the same `WithinTransaction` as the state change,
  always.
- **Backoff windows.** Compressed retry windows mean the e2e suite can drive a full
  `open → uncollectible` cycle in under a minute. Make them env-configurable so this stays true.

---

## 11. Docs to update on completion

- `README.md`: Billing Service section 🚧 → ✅ with the actual (not target) endpoints and events.
- `PLAN.md`: status table gains `billing-service`; a dated section describing what landed, the
  deferred list, and the fresh-volume caveat.
- `CLAUDE.md`: money-representation invariant (§2) as a repo-wide rule; billing's data model.
