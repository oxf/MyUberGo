# MyUberGo — Uber Microservices Clone in Go

A distributed, event-driven clone of Uber built in Go to learn microservices patterns, event streaming, and clean software architecture.

---

## 🏗️ Architecture Overview

The system is designed as 8 services communicating asynchronously via **Apache Kafka** (event streaming) and synchronously via **REST/HTTP**. **6 are implemented today** (Auth, Ride, Matching, Driver, Billing, Location — Location is Slice 1 of 4, see §6 below); **1 is planned** (Notification); an API Gateway (Kong) fronts the 6 implemented services — see status markers in the section below.

### System Architecture Diagram (target design)

![Ride-Hailing Microservices Architecture](content/diagram.png)

---

## 📁 Repository Structure & Services

Each service below lists its **target** responsibilities, schema, and contracts. Where a service is already implemented, its *current* code is a simpler subset of what's described — check `services/shared/migrations/sql/` and `services/contracts` for what actually exists today; this README describes where each service is headed.

### 1. Auth Service — ✅ Implemented (Port `:8000`)

Manages user registration/login, JWT issuance (access + refresh), token refresh, and profile lookup.

**Schema**: `USER` (id, email, password_hash, name, phone, role, timestamps, soft-delete via `deleted_at`), `REFRESH_TOKEN` (id, user_id, token, expires_at, revoked_at).

**HTTP (target)**: `POST /api/auth/register`, `POST /api/auth/login`, `POST /api/auth/refresh`, `GET /api/auth/me`, `POST /api/auth/logout`.

**Kafka**: none published or consumed.

> Current code implements `POST /signup`, `POST /login`, `POST /refresh`, `GET /me`, `POST /logout` (no `/api` prefix, different route names than the target above) against `auth.user`/`auth.refresh_token`, with soft delete. It also goes beyond this target schema: `auth.user` is now the shared login identity for a third role, `Admin` (gated to no signup path — seeded directly in `0002_auth.up.sql`), and each role's own data lives in a separate table keyed by `user_id` — `auth.client`/`auth.admin` here, `driver.driver` in the Driver Service. A `Client`'s access token carries a `client_id` claim (that role table's id, not the user's), which Kong injects as `X-Client-Id` for `ride.ride.client_id` — see `CLAUDE.md`'s "Data model" and "Auth" sections for the current shape.

### 2. Ride Service — ✅ Implemented (Port `:8001`)

Creates ride requests, estimates fare/distance from tariffs, manages the ride state machine, and reliably publishes ride events via the transactional outbox pattern.

**Schema**: `RIDE` (client/driver ids, status machine `Requested → Matched → InProgress → Completed`/`Cancelled`, timestamps per transition, `bill_id`, `rating_id`), `RIDE_REQUEST` (pickup/destination geometry, distance, price, tariff), `TARIFF` (time-of-day pricing: Morning/Noon/Evening/Weekend), `OUTBOX_EVENTS`.

**HTTP (target)**: `POST /api/rides`, `GET /api/rides/{rideId}`, `DELETE /api/rides/{rideId}` (cancel).

**Kafka publishes**: `ride.requested`, `ride.cancelled`.
**Kafka subscribes**: `ride.accepted` (→ set driver/matched_at/status), `ride.started`, `ride.finished`, `payment.completed` (→ set bill_id).

> Current code implements `POST /request-ride`, `GET /ride`, `GET /ride/{id}`, `DELETE /ride/{id}` (cancel; no `/api` prefix, different route names than the target above) with a flat `ride.ride` table. `ride.tariff` is now read: fare is `base_fare_minor + price_per_km_minor*distanceKm + price_per_min_minor*durationMin` (haversine distance, an assumed average speed for duration), all in integer minor units + an ISO-4217 `currency` — see CLAUDE.md's money-representation invariant. `POST /request-ride` accepts an optional `tariffName` (defaults `"Standard"`); a `"Standard USD"` tariff is also seeded, so multi-currency is exercisable end-to-end. Publishes `ride.requested`/`ride.completed`/`ride.cancelled` (all now carrying `clientId` + the money fields billing-service needs) and consumes `ride.accepted` (→ `Matched`) and `payment.completed` (→ sets `bill_id`, closing the loop billing-service's `ChargeWorker` opens). Cancellation fee is a stub flat 300 minor units, charged only when a driver was already assigned — real per-distance/per-time fee logic is still future work, but the path is no longer permanently dead.

### 3. Matching Service — ✅ Implemented, simplified algorithm (Port `:8002`)

Consumes ride/shift events and matches ride requests to available drivers. Target design is a full radius-search + weighted-ranking + tiered-broadcast + atomic-accept + expanding-retry algorithm (see [Matching Algorithm](#-matching-algorithm-target-design) below); current code implements a simplified, rating-only version of that same shape, backed entirely by Redis (no Postgres — matching-service has never had a Postgres dependency in the live code path, and it was formally removed from `go.mod`/`docker-compose.yml`).

**Redis schema (current)**: `ride:{id}` (hash: pickup/destination/price/status), `drivers:online` (ZSET driverId→rating — the matchable pool), `driver:{id}` (hash: shiftID/status/rating), `ride:{id}:offered_drivers` (SET, TTL 1h), `ride:{id}:accepted_by` (STRING, `SET NX` claim, TTL 1h), `ride:{id}:cancelled` (set by the `ride.cancelled` consumer on cancellation), `driver:{id}:current_offer` (STRING rideId, TTL 30s), `driver:{id}:notifications:minute` (rate limit, sliding 60s window), `pending_ride:{id}` (retry state: attempt + deadline).

**HTTP**: `POST /rides/{rideId}/accept` (driver claims a ride; atomic `SET NX` — 409 if already taken, 400 if expired/cancelled/not offered to this driver, 404 if the ride doesn't exist), `GET /drivers/{driverId}/offer` (poll-based — no Notification service exists yet to push offers, so drivers poll for their current offer; 404 when there is none).

**Kafka subscribes (current)**: `ride.requested`, `shift.updated`, `ride.cancelled` (clears offer/pending state, sets `ride:{id}:cancelled`, and restores the driver to `drivers:online` if the ride had already been matched — using matching-service's own Redis `AcceptedBy` as the source of truth rather than the driver id carried on the event, since that reflects ride-service's Postgres row and can lag).
**Kafka publishes**: `ride.accepted` (published directly from the accept handler, no outbox — Redis has no transaction to hide a dual write behind, so a crash between the Redis match and the Kafka publish loses the event; an accepted at-most-once tradeoff since the match itself is durable in Redis).

> Simplifications vs. the target design below: **discovery is geo-first with a rating-only fallback** (changed 2026-08-12, once the Location service's Slice 1 shipped) — `BroadcastOffersHandler` calls Location's `GET /internal/drivers/nearby` for geographic candidates, intersects against `drivers:online` (Location doesn't track shift state), and ranks by `0.5×distance + 0.5×rating` (README's `acceptance_rate` term has no data source yet); any failure in that path falls back to the original rating-only `drivers:online` ZSET query, never failing the ride. `driver.rating` is still carried on an enriched `shift.updated` event either way; a driver enters the pool on `status: "Online"` and leaves it on any other status (including `"Ended"` — this used to be silently dropped by a driver-service bug that's now fixed, see below). **Broadcasting is BROADCAST-only** (top 5 at once, first accept wins) — TIERED escalation and concurrent goroutine/channel fan-out are deferred to a later Stage-3 pass. **Retry widens the candidate pool** (`attempt × 5` drivers queried) instead of expanding a geo radius, capped at 5 attempts via a `MatchRetryWorker` background sweep (ticker+select loop over `pending_ride:*`); giving up marks the ride `failed` and logs — there's no Notification service to tell the client. **Rate limiting** is a sliding 60s window (each offer resets the TTL), not a strict fixed window, but caps at 3/minute as designed. `ride.cancelled` is now produced by ride-service and consumed here (see above) — the `ride:{id}:cancelled` key is actually set, not just checked. `ride-service` now consumes `ride.accepted` too (see the Ride Service section above), so a matched ride's Postgres status does flip to `Matched`.

### 4. Driver Service — ✅ Implemented (Port `:8003`)

Manages driver profiles, shifts (start/end, earnings), and ride start/finish actions. This is the service furthest along the Clean Architecture / CQRS refactor (see `CLAUDE.md`).

**Schema**: `DRIVER` (license number/expiry, vehicle type/plate/color, rating, acceptance_rate, status: offline/online/assigned/on_ride, current_shift_id), `SHIFT` (started/ended, total_rides, total_earnings), `DRIVER_RATING`.

**HTTP (target)**: `POST /api/shifts/start`, `POST /api/shifts/end`, `POST /api/rides/{rideId}/start`, `POST /api/rides/{rideId}/finish`, `GET /api/drivers/available?lat&lon&radiusKm` (internal, used by Matching).

**Kafka publishes**: `shift.started`, `shift.ended`, `ride.started`, `ride.finished`.
**Kafka subscribes**: `ride.accepted` (→ status=Assigned).

> Current code implements `POST /driver`, `PUT /driver/{id}`, `GET /driver`, `GET /driver/{id}`, `POST /driver-shift/create`, `PUT /driver-shift/{id}`, `GET /driver-shift`, `GET /driver-shift/{id}` (no `/api` prefix, different route/table names than the target above — the table is `driver.driver`, renamed from `driver_profile`) — driver CRUD and shift create/update, with a working transactional-outbox worker publishing `shift.updated` events to Kafka (now carrying the driver's `rating`, consumed by matching-service's rating-only ranking — see below). `driver.driver` no longer stores `name`/`phone` either (dropped in favor of `auth.user` as the single source — see the Auth Service section above); `GET /driver`/`GET /driver-shift` are Admin-only at the Kong gateway. It doesn't yet have license/vehicle detail fields, `acceptance_rate`/`current_shift_id`, the ride start/finish endpoints, or the `available` nearby-driver endpoint. It now consumes both `ride.accepted` and `ride.cancelled`, but both handlers are logging-only placeholders — there's no persisted "driver is on a ride" state yet (`driver.driver.status` only allows `Offline`/`Online`), so there's nothing for either handler to actually flip. A bug where setting a shift to `Ended` never published a `shift.updated` event at all (the handler returned before reaching the outbox insert) is fixed — matching-service's online-driver pool now correctly shrinks when a driver goes offline.

### 5. Location Service — 🚧 Partially implemented (Slice 1 of 4, Port `:8004`)

Ingests driver GPS pings, keeps live position in a Redis geo index, and serves geospatial discovery to Matching. Target design (below) also covers WebSocket live tracking, a NoSQL raw-history archive + map-matched ride summaries, and a Geoapify geocoding/routing proxy — none of that is built yet; see `docs/location/LOCATION_SPEC.md` for the full 4-slice design and current status.

**Schema (target)**: Redis `driver:{id}:location` (TTL 5m), `ride:{id}:locations`; NoSQL `RideLocationHistory` (PK driver_id, SK timestamp_ms, GSI by ride_id, 30-day TTL); Postgres `RIDE_SUMMARY_LOCATION` (start/end location, route polyline, total distance).

**HTTP (target)**: `POST /api/location`, `GET /api/location/distance`, `GET /api/location/drivers/nearby` (internal).

**WebSocket (target)**: client → server `LocationUpdate` every 3-5s; server → both parties `DriverLocationUpdate`/`ClientLocationUpdate`.

**Kafka subscribes (target)**: `ride.started` (begin tracking), `ride.finished` (archive + build polyline + clean up Redis).

> Current code (Slice 1 only, 2026-08-12): Redis-only, no Postgres/NoSQL/WebSocket yet. Schema is `loc:drivers:geo` (GEO index), `loc:drivers:lastseen` (ZSET, feeds a `StalenessWorker` sweep — Redis GEO has no per-member TTL), `loc:driver:{id}` (HASH, TTL 5m), plus a `loc:driver:{id}:owner`/`loc:user:{userId}:driver` identity-mapping pair cached from `shift.updated`, all `loc:`-prefixed to avoid colliding with matching-service's own unprefixed keys in the same shared Redis. HTTP is `POST /api/location/batch` (client-facing, no `driverId` in the body — identity is resolved from the caller's Kong-injected `X-User-Id`) and `GET /internal/drivers/nearby` (network-isolated, no Kong route — matching-service only). No WebSocket, no `ride.started`/`ride.finished` consumers, no history archive, no Geoapify proxy. Matching Service (above) is the one live consumer, with a fallback to its pre-existing rating-only pool if Location is unreachable.

### 6. Billing Service — ✅ Implemented (Port `:8005`)

Turns completed/fee-bearing rides into invoices, collects them through a pluggable payment provider, and records every money movement in a double-entry ledger. A real Stripe adapter (test-mode only) now exists alongside the original in-process stub, selected by the `PAYMENT_PROVIDER` env var — see `docs/billing/BILLING_SPEC.md` for the full design and what's still deliberately deferred (§9 there).

**Schema** (`billing` schema in `services/shared/migrations/sql/0006_billing.up.sql`): `customer` (one row per client per provider), `payment_method` (brand/last4 display metadata only — never a PAN/CVC; a partial unique index enforces at most one active default per client), `invoice` (`ride_fare`/`cancellation_fee`, `open`/`paid`/`uncollectible`/`void`; **`UNIQUE (ride_id, type)`** is the idempotency guard against redelivered Kafka events), `invoice_line`, `payment` (one row per collection attempt, deterministic `idempotency_key`), `ledger_account`/`ledger_transaction`/`ledger_entry` (append-only double-entry bookkeeping — account types `client_receivable`/`driver_payable`/`platform_revenue`/`psp_clearing`/`psp_fees`/`bad_debt`, one account per `(type, owner, currency)`), `outbox_message`.

**HTTP**: `POST /payment-methods`, `GET /payment-methods`, `DELETE /payment-methods/{id}` (caller-scoped via Kong's `X-Client-Id`); `GET /invoices/{id}`, `GET /rides/{rideId}/invoice` (caller-scoped, authorized in-service against the invoice's own `client_id`); `GET /invoices` (Admin-only at Kong, paged); `GET /ledger/balance?type&currency&ownerId` (Admin-only — the cheapest possible regression check for the ledger invariants).

**Kafka subscribes**: `ride.completed` → `CreateInvoiceFromRide` (`type=ride_fare`) + posts the `invoice_opened` ledger transaction (client_receivable debited; platform_revenue + driver_payable credited, split by `PLATFORM_COMMISSION_BPS`, truncating division so the two always sum exactly to the fare). `ride.cancelled` → same pipeline with `type=cancellation_fee` when `feeMinor > 0` (100% to `platform_revenue`, no `driver_payable` leg) — skipped entirely when the fee is zero, which is most cancellations today.
**Kafka publishes**: `payment.completed`, `payment.failed` (via the same transactional-outbox pattern as ride/driver-service).

A `ChargeWorker` (ticker+`select`, same shape as the outbox workers) sweeps invoices whose `next_attempt_at` has elapsed, calls the `PaymentProvider` port — either `StubProvider` (picking its outcome from the payment method's token: `pm_stub_ok` succeeds, `pm_stub_decline`/`pm_stub_insufficient` fail with the matching code) or `StripeProvider` (a real, test-mode-only Stripe adapter — `PaymentIntents.Create`, confirmed off-session), whichever `PAYMENT_PROVIDER` selects; a client with no active payment method fails with `no_payment_method` regardless of provider — and posts `payment_succeeded` (psp_clearing debited, client_receivable credited) on success. On failure it backs off (`PAYMENT_BACKOFF`, default `1m,5m,30m`) and retries up to `MAX_PAYMENT_ATTEMPTS` (default 3), after which the invoice goes `uncollectible` and posts `invoice_uncollectible` (bad_debt debited, client_receivable credited) — `driver_payable` deliberately stays posted from the first transaction, since the driver is still owed money the platform never collected; that divergence is the entire reason for double-entry bookkeeping over a single balance column.

The `PaymentProvider` port speaks its own vocabulary (`ChargeResult{Status, ProviderIntentID, FailureCode, FailureMessage}`), not Stripe's — the whole point was that a real Stripe adapter could become an implementation of this port with no domain refactor, and that's exactly what happened: `StripeProvider` (`internal/infrastructure/payment/stripe`) implements the same port, plus a `WebhookHandler`/`psp_event` inbox for Stripe's async `payment_intent.*` callbacks (`POST /webhooks/stripe`, exposed publicly through Kong when `PAYMENT_PROVIDER=stripe`). Still deferred and why it stays additive: a reconciliation poller for payments stuck in `processing` (needs real async provider data to sweep — now unblocked now that webhooks exist, just not built yet), driver payouts (`driver_payable` is posted from day one specifically so this doesn't need a schema backfill later), refunds/disputes/wallet/promos/FX. See `docs/billing/BILLING_SPEC.md` §9 for the full list.

### 7. Notification Service — 🚧 Planned (target port `:8006`)

Fans out push/SMS/email notifications for every ride/shift/payment lifecycle event.

**Schema**: `NOTIFICATION` (user_id, type: push/sms/email, template, payload, status: queued/sent/delivered/failed).

**Kafka subscribes to everything**: `ride.requested`, `ride.accepted`, `ride.started`, `ride.finished`, `ride.cancelled`, `payment.completed`, `shift.started`, `shift.ended` — each mapped to a specific notification to client and/or driver (see table in-code once implemented).

### 8. API Gateway — ✅ Implemented (Kong, port `:8090`)

Single entry point: Kong (`gateway/kong.yml`) validates the JWT issued by Auth Service via its `jwt` plugin, and a `post-function` Lua snippet extracts claims and injects `X-User-Id`/`X-User-Email`/`X-User-Role`/`X-Client-Id` headers (overwriting any the caller sent directly), routes by path prefix, and rate-limits (per-user, per-IP, per-endpoint — see `gateway/kong.yml`'s plugins). Notification service routing isn't relevant yet since that service doesn't exist.

**Routes (actual)**: `/api/auth/{signup,login,refresh}` → Auth (public, no token); `/api/auth/{me,logout,users}`, `/api/ride/*`, `/api/driver/*`, `/api/billing/*`, `/api/matching/*`, `/api/location/*` → their respective services (protected, `jwt` + header injection); `GET /api/auth/users`, `GET /api/ride`, `GET /api/driver/driver`, `GET /api/driver/driver-shift`, `GET /api/billing/invoices`, `GET /api/billing/ledger/balance` → Admin-only (adds a `require_admin` check). See CLAUDE.md's "API Gateway (Kong)" section for the full route-precedence rules.

> Downstream services trust the gateway-forwarded headers (e.g. `ride-service` reads `X-User-Id`/`X-Client-Id` directly) because Kong is the sole ingress — none of the 6 implemented services publish a host port on their own.

---

## 🎯 Matching Algorithm (target design)

The full design for `matching-service`'s ride-to-driver matching. Current code implements a simplified version of this shape (rating-only ranking, BROADCAST-only, pool-widening retry instead of radius expansion) — see the "Matching Service" section above for exactly what's simplified and why.

1. **Discovery** — on `ride.requested`, query online, available drivers within a radius (default 5km bounding box).
2. **Ranking** — score each candidate: `0.4×distance_score + 0.4×rating_score + 0.2×acceptance_score` (all normalized 0-1), sort by score desc then distance asc.
3. **Broadcasting** — configurable strategy: `BROADCAST` (top 5 at once, first accept wins), `SEQUENTIAL` (one at a time with a timeout per driver), or `TIERED` *(recommended)*: top 2 high-rated drivers (10s) → next 5 (15s) → next 10 (20s), escalating only on timeout.
4. **Atomic acceptance** — a driver's accept does `SET ride:{id}:accepted_by = driver_id NX`; only the first successful `SET` wins, everyone else gets 409. Cancelled/expired offers are rejected before the atomic claim.
5. **Retry with expanding radius** — if nobody accepts within the window, retry from Discovery with radius +2km (5→7→9→11→13, capped at 15km) up to 5 attempts, then give up and notify the client.
6. **Rate limiting** — cap offers per driver at e.g. 3/minute (`driver:{id}:notifications:minute`, TTL 60s) so a busy driver isn't spammed; skip to the next-ranked driver instead.

---

## 🔁 End-to-End Event Flow (target design)

![Kafka Event Routing](content/kafka-flow.png)


## 🚕 Full ride  lifecycle, client request to payout:

![Ride Lifecycle](content/ride-lifecycle.png)

---

### Infrastructure Stack

The infrastructure runs locally via Docker Compose:
*   **PostgreSQL 15**: Primary relational storage shared across services (using separate schemas/databases).
*   **Redis 7 (Alpine)**: Cache and fast-access session/state store; target store for Matching's offer/accept state and Location's live GPS cache.
*   **Apache Kafka**: Distributed event streaming platform.
*   **Kafka UI**: Web dashboard for inspecting Kafka topics, consumer groups, and messages (accessible on Port `:8080`).
*   **DynamoDB/Cassandra** *(planned)*: time-series store for Location Service's GPS history (not yet in `docker-compose.yml`).

---

## 🗺️ Architectural Transition: Single File to Clean DDD

To facilitate learning, the services are designed in progressive architectural complexity:

1.  **Phase 1 (Procedural Scripting)**: Implement the core service in a single `main.go` file. Focus on speed, database integrations, HTTP handler mapping, and event consumer setups.
2.  **Phase 2 (Layered / Clean Architecture)**: Move logic out of HTTP handlers and raw SQL queries into distinct architectural boundaries:
    *   **Domain Layer**: Pure business logic (entities, value objects, domain rules). Independent of databases or framework libraries.
    *   **Application Layer**: Use cases, command and query handlers (CQRS), ports (interfaces for repositories, message publishers).
    *   **Infrastructure/Persistence Layer**: Adapters implementing the ports (PostgreSQL repositories, Kafka publishers, Redis caching).
    *   **Interfaces/HTTP Layer**: Routers, controllers, request parsing, and response rendering.

`driver-service`, `matching-service`, and `ride-service` are all fully in Phase 2 now; `auth-service` is still Phase 1.

---

## 🚀 Development & Refactoring Roadmap

> For the ordered, checkable step-by-step task list (what's being worked on right now), see **[PLAN.md](./PLAN.md)**. The phases below are the high-level roadmap; PLAN.md breaks the current phase into small steps.

### Phase 1: Clean Architecture Refactoring (Core Services)
*   [x] `driver-service` refactored to CA/CQRS.
*   [x] `matching-service` refactored to CA/CQRS + simplified matching algorithm implemented (discovery/broadcast/atomic accept/retry/rate limiting against Redis). Geo-radius discovery landed 2026-08-12 (Location service Slice 1) — ranking is now `0.5×distance + 0.5×rating` when Location is available, rating-only on fallback. Remaining: TIERED broadcast strategy, Stage-3 concurrent fan-out (goroutines/channels for the broadcast step).
*   [x] **Refactor `ride-service`**: `ride-service` is already Stage 2 (CQRS/DDD layered, not Stage 1 as this line previously implied) and the `Cancelled` cancellation flow (`DELETE /ride/{id}`, `ride.cancelled` publish/consume) is now implemented — see the Ride/Matching/Driver Service sections above. Remaining: add `RIDE_REQUEST`/`TARIFF` tables and time-of-day pricing (schema has `ride.tariff` but it isn't read), and a real (non-stub) cancellation fee calculator once Billing exists.
*   [x] **Refactor `auth-service`** (2026-07-25): moved JWT generation and password hashing into domain services (`PasswordHasher`/`TokenIssuer` ports), added `updated_at`/`deleted_at` soft delete, `GET /api/auth/me`, `POST /api/auth/logout`.

### Phase 2: Gateway Configuration & Inter-service Auth
*   [x] Scaffold and implement the **API Gateway** (Kong, `gateway/kong.yml`): JWT validation via the shared secret, `X-User-Id`/`X-User-Email`/`X-User-Role`/`X-Client-Id` header injection, path-based routing table, rate limiting — see §8 above.
*   [x] Secure downstream service communication — Kong is the sole ingress for all 6 implemented services (none publish a host port), so downstream services trust the gateway-forwarded headers without re-validating the JWT themselves.

### Phase 3: New Feature Services
*   [ ] **Location Service** (Slice 1 of 4 done, 2026-08-12 — see the Location Service section above and `docs/location/LOCATION_SPEC.md`): driver ping ingest + Redis geo index + nearby-drivers query, consumed by Matching. Remaining: WebSocket live tracking, NoSQL history with TTL + ride-id GSI, map-matched ride summaries, `ride.started`/`ride.finished`-driven tracking windows (opens on `ride.accepted` instead, deliberately — see the spec), Geoapify proxy.
*   [x] **Billing & Payment Service** (2026-07-25, Stripe adapter 2026-08-01): payment methods, `ride.completed`/`ride.cancelled` consumers, invoice/ledger generation, `payment.completed`/`payment.failed` publishers. Remaining (deferred per `docs/billing/BILLING_SPEC.md` §9): driver payouts/Connect, wallet/credits/refunds, FX, a reconciliation poller for payments stuck in `processing`.
*   [ ] **Notification Service**: consume every ride/shift/payment event and fan out push/SMS/email per template, with delivery-status tracking.

### Phase 4: Production Quality & Observability
*   [x] Standardize structured logging using `logrus` across all services — JSON to stdout, level from `LOG_LEVEL`, trace_id/span_id correlation (`services/observability/obslog`).
*   [x] Implement OpenTelemetry tracing to trace requests end-to-end from the Gateway through PostgreSQL and Kafka — Kong originates the trace via its `opentelemetry` plugin, all 5 services propagate it (including through the transactional outbox, via a persisted `trace_context` column — see CLAUDE.md's "Observability (OpenTelemetry)" section). Traces land in Tempo, viewable in Grafana at `http://localhost:3000`.
*   [x] Set up metrics for HTTP response rates/latencies and Kafka message consumption times — via the OTel Collector's `spanmetrics`/`servicegraph` connectors and `kafkametrics` receiver (RED per service/operation, consumer-group lag), exported to Prometheus; see `observability/grafana/dashboards/` for the provisioned Platform Overview, Ride Funnel, Kafka & Outbox, and Billing dashboards. Deliberately not a `/metrics`-per-service Prometheus scrape endpoint — everything pushes to the Collector over OTLP instead, one protocol end-to-end.

---

## 🛠️ Quick Start

1.  **Spin up infrastructure and services**:
    ```bash
    docker-compose up --build
    ```
2.  **Inspect database migrations**:
    Schemas are defined in `services/shared/migrations/sql/` (golang-migrate) and applied automatically by the `migrate` compose service before the app services start.
3.  **Inspect events**:
    Navigate to the Kafka UI at `http://localhost:8080` to view active topics and consumer groups.
