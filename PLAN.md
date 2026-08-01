# PLAN.md — Implementation Roadmap

Living, checkable roadmap for MyUberGo. `README.md` describes the target product/architecture; `CLAUDE.md` is the operating guide for Claude Code sessions; this file is the working task list — update it as steps complete or priorities change.

## The 3-stage learning progression

Every service moves through the same 3 stages, deliberately, one at a time:

1. **Stage 1 — Basic/procedural**: one `cmd/main.go`, handlers run raw SQL directly, no layering.
2. **Stage 2 — CQRS + DDD (Clean Architecture)**: `domain`/`application` (`command`+`query`)/`persistence`/`interfaces`/`infrastructure` layering, decorator-wrapped handlers (logging + metrics), context-propagated `TransactionManager`, graceful shutdown, health checks. `driver-service` is the reference implementation — copy its shape.
3. **Stage 3 — Production-grade advanced Go** (not started anywhere yet): idiomatic concurrency (goroutines/channels/`select` — e.g. fan-out ride offers to N drivers concurrently, worker-pool outbox pollers), code generation from OpenAPI (HTTP) and/or gRPC (inter-service calls) instead of hand-written contracts, a real metrics/tracing backend, a real liveness probe.

## Current status per service

| Service | Stage | Notes |
|---|---|---|
| auth-service | 2 | full CQRS/DDD layering mirroring ride-service, no outbox (auth publishes no events) but now has a `TransactionManager` (signup writes `auth.user`+`auth.client` atomically); JWT/bcrypt behind `PasswordHasher`/`TokenIssuer` ports; adds `GET /me`, `POST /logout`, soft-delete columns on `auth.user`, and the `Admin` role (see the two 2026-07-25 sections below) |
| ride-service | 2 | full CQRS/DDD layering (`domain`/`application`/`persistence`/`interfaces`/`workers`), decorator-wrapped handlers, transactional-outbox worker for `ride.requested`, plus a `ride.accepted` consumer (see 2026-07-21 section below) |
| driver-service | 2 (+ early Stage 3 features) | full CQRS/DDD layering; graceful shutdown + health checks + logging/metrics decorators + a working transactional-outbox worker already present, but metrics is a logging-only stub (no Prometheus) and health's liveness check is hardcoded `true` (never fails) — real Stage 3 work still needed there |
| matching-service | 2, complete for its current scope | full CQRS layering, HTTP layer, Kafka producer, graceful shutdown, Redis health checks; implements a simplified version of the README's matching algorithm (rating-only ranking, BROADCAST-only, pool-widening retry) — see the 2026-07-19 section below |
| billing-service | 2 | full CQRS/DDD layering copying ride-service's shape; transactional-outbox worker for `payment.completed`/`payment.failed`; a `ChargeWorker` (same ticker+select shape) sweeping open invoices through a pluggable `PaymentProvider` (stub or a real, test-mode-only Stripe adapter, via `PAYMENT_PROVIDER`); double-entry ledger with append-only entries — see the 2026-07-25 and 2026-08-01 sections below |
| e2e-test | n/a (tooling, not a service) | continuous client-activity simulator: N virtual clients + M virtual drivers (goroutine-per-actor) drive auth/driver/ride/billing over HTTP with deep read-back verification and periodic stats; run manually via `go run ./cmd` against the compose stack — deliberately NOT in docker-compose. See `services/e2e-test/README.md` |

## Admin dashboard + paged/sorted list endpoints (2026-07-18)

- [x] `contracts/http`: `PagedResponse[T]` envelope, `UserDto`; dropped phantom `ShiftDto.Status`; `endedAt` nullable
- [x] auth-service: new `GET /users` (paged/sorted, never exposes password_hash)
- [x] ride-service: fixed 1-based paging off-by-one (first page was skipped by default), sort whitelist, totalCount
- [x] driver-service: `PageRequest` + sort whitelists through query/persistence; count queries; GET endpoints normalized to camelCase DTOs with RFC3339 timestamps
- [x] e2e-test: envelope adoption; new `auth.users.list` / `driver.shift.list` verifies (all green against the live stack)
- [x] Bruno: list requests updated; `Get Users`, `Get Driver Shift List` added
- [x] `web/`: Vite+React+TS dashboard (DataTable/Pagination/usePagedQuery, URL-backed state, dev-proxy → no CORS needed)

## Fixed this pass (2026-07-18)

Found by building the e2e-test simulator — auth-service login/refresh had never been called programmatically and were entirely broken:

- `services/auth-service/cmd/main.go` login: queried `FROM "user"` without the `auth.` schema qualifier (DSN sets no search_path → relation not found → every login 401'd) and scanned the UUID `id` into an `int` → fixed to `auth."user"` and `string`.
- Same file: the refresh-token INSERT was also missing the `auth.` qualifier, so tokens were never stored (only a log line) and `/refresh` always 401'd → fixed, including the existence check.
- Same file: JWT `user_id` claim was created/parsed as an int (UUID can't survive the float64 round-trip) → claim is now the UUID string end-to-end.
- Same file: `/refresh` encoded a bare JSON string instead of `contracts.RefreshResponse` → now returns the contract shape.

## Fixed this pass (2026-07-14)

- `services/contracts/kafka/driver-service.go`: `ShiftUpdatedEvent.DriverID` json tag was `clientId` → fixed to `driverId`.
- `services/driver-service/.../shift_handler.go`: `GetList`/`GetByID` were calling the driver-profile queries instead of the shift queries → fixed to call `GetShiftList`/`GetShiftByID`.
- `services/driver-service/.../shift_handler.go`: `Update` was using the request body's `DriverId` as the shift ID instead of the path `{id}` → fixed to use the path value.
- **`services/driver-service/internal/workers/shift_updated_outbox_worker.go` implemented** (was a non-compiling stub — see below).

## driver-service: outbox worker implemented (2026-07-14)

The previous stub (dead procedural copy-paste, undefined package vars, never wired into `main()`) is now a real Stage-2 layered worker:
- `domain.OutboxRepository` extended with `GetUnprocessedBatch`/`MarkProcessed`/`IncrementRetries` (implemented in `internal/persistence/outbox_postgres_repository.go`, all tx-aware via the existing `Executor(ctx, db)` helper).
- New port `services.EventPublisher` (`internal/application/services/event_publisher.go`) + Kafka adapter `internal/infrastructure/kafka.Publisher` (one persistent `kafka.Writer`, unlike ride-service's per-batch writer).
- `workers.OutboxWorker` (renamed from `ShiftUpdatedOutboxWorker` — it's topic-agnostic, drains whatever's in the outbox): ticker + `select` loop, each batch runs inside `transaction.WithinTransaction` so `FOR UPDATE SKIP LOCKED` + publish + mark-processed/increment-retries share one transaction. Failed publishes increment `retries` and are retried next tick (no max-retry/dead-letter yet — future work).
- `shutdown.Manager` gained `OnStop(fn func())` so `main.go` can cancel the worker's context before waiting on the shutdown WaitGroup (previously a looping worker had no way to be told to stop).
- Fixed the producer/consumer topic mismatch: `UpdateShiftHandler` now writes outbox rows with topic `shift.updated` (was `driver.shifts.updated`), matching what `matching-service`'s `ShiftUpdatedConsumer` actually subscribes to.
- `cmd/main.go` also had ~530 lines of dead Stage-1 procedural handlers/helpers (never registered as routes) deleted as part of this pass — the file is now just the Stage-2 composition root.
- `go build ./... && go vet ./...` pass clean for the whole service (previously failed on the workers package).

Follow-ups not done yet: max-retry/dead-letter handling for permanently-failing messages; `ride-service` should adopt this same layered worker pattern when it moves to Stage 2 (see below).

---

## Next target: matching-service — finish Stage 2, build the real algorithm

Small, ordered, checkable steps. Do them roughly in order; each should be independently buildable/runnable.

### A. Stage 2 cleanup (finish the CQRS migration)
1. [x] Delete the dead code in `cmd/main.go`: `startRideRequestedConsumer`, `handleRideRequested`, and the now-unused `db *sql.DB` / `kafkaBroker string` package vars and their `sql.Open`/`lib/pq` import.
2. [x] Matching-service's target design (README) is **Redis-only** — no Postgres. Confirm and drop the Postgres dependency entirely once step 1 is done (no other file in this service uses `db`).
3. [x] Fix consumer naming/behavior: rename `RideAcceptedConsumer` → something accurate (it consumes `ride.requested`) or split so a future real `ride.accepted`-consuming type isn't confused with this one. Stop hardcoding the topic inside `Run()`; use the `topic` parameter that's already passed in (it's currently accepted and ignored). Done as `RideRequestedConsumer`, now `ctx`-aware for graceful shutdown too.
4. [x] Add query handlers as needed once the accept endpoint (below) requires reading offer/ride state back out of Redis. Done as `GetDriverOffer` (backs the `GET /drivers/{driverId}/offer` polling endpoint).

### B. Build the matching algorithm incrementally
Implement in this order, each as its own small command/handler:

5. [x] **Discovery (simplified)** — done via the third option this step raised, not the two originally listed: rather than reading `driver.driver_profile` directly (keeps Postgres, rejected) or adding a driver-service HTTP endpoint (extra service call), `ShiftUpdatedEvent` was enriched with `Rating float64`, and matching-service maintains its own `drivers:online` Redis ZSET (driverID → rating) kept in sync by `UpsertDriver` (renamed from `CreateDriver`). Fully Redis-only, no cross-service read. Ranking is rating-only, no distance component (no Location service yet).
6. [x] **Offer state in Redis** — implemented the target key schema (`ride:{rideId}:offered_drivers`, `ride:{rideId}:accepted_by`, `ride:{rideId}:cancelled`, `driver:{driverId}:current_offer`) plus two additions not in the original list: `driver:{driverId}:notifications:minute` (rate limit) and `pending_ride:{rideId}` (retry state: attempt + deadline) — both needed once retry/rate-limiting (steps 8-9) were designed in.
7. [x] **Atomic accept endpoint** — `POST /rides/{rideId}/accept` via Redis `SET ... NX`; 409 on race loss, 400 on expired/cancelled/not-offered-to-this-driver, 404 if the ride doesn't exist.
8. [x] **Retry with expanding attempts** — implemented as pool-widening (`attempt × 5` candidates queried, not a shrinking rating threshold as this step originally suggested — pool-widening turned out simpler and reuses the same ranking path). Capped at 5 attempts via a background `MatchRetryWorker` (ticker+`select` sweep over `pending_ride:*`); gives up and marks the ride `failed` after the max.
9. [x] **Rate limiting** — `driver:{driverId}:notifications:minute` via Redis `INCR`/`EXPIRE`, cap 3/minute, skips over-limit drivers when broadcasting. Implemented as a sliding window (TTL resets on each offer) rather than a strict fixed window — simpler, and the difference doesn't matter at this scale.
10. [x] **Publish `ride.accepted`** — first Kafka producer in this service, published directly from `AcceptRideHandler` (no outbox — Redis has no transaction to hide a dual write behind; log-and-continue on publish failure is an accepted at-most-once tradeoff since the match itself is already durable in Redis). `contracts.RideAcceptedEvent{RideID, DriverID, AcceptedAt}` added to `services/contracts/kafka`.

### C. Stage 3 preview (once A+B work end-to-end)
11. [ ] Convert the offer broadcast (step 6/7) to use goroutines + a channel + `select`: fan out push-style "you've been offered this ride" sends to the top-N ranked drivers concurrently, and `select` between an acceptance signal and a timeout instead of blocking sequentially. This is the natural first place to apply the concurrency patterns from Stage 3. Not started — A+B are done end-to-end now, so this is unblocked.

---

## matching-service: Stage 2 complete + simplified matching algorithm (2026-07-19)

All of §A and §B above landed in one pass. Also fixed along the way: `driver-service`'s `UpdateShiftHandler` set-to-`Ended` path returned before reaching the outbox insert, so ending a shift never published a `shift.updated` event — matching-service's `drivers:online` pool would only ever grow. Both the `Ended` and normal-status paths now share one transaction and one outbox-insert tail.

End-to-end flow now working: `ride.requested` → cache ride + broadcast top-5 rating-ranked online drivers → driver polls `GET /drivers/{driverId}/offer` → `POST /rides/{rideId}/accept` (atomic claim) → `ride.accepted` published. Unmatched rides retry (widening pool) up to 5 attempts before being marked `failed`. e2e-test simulator's driver actors now exercise this whole loop (`matching.offer.get`, `matching.ride.accept`, `matching.ride.accept.dup` ops) with 404/409 deep verification.

Follow-ups not done yet (tracked, not urgent):
- ~~`ride-service` doesn't consume `ride.accepted` yet~~ — done, see the 2026-07-21 section below.
- ~~No `ride.cancelled` producer/consumer anywhere~~ — done, see the 2026-07-23 section below.
- TIERED broadcast strategy (README's recommended design) and geo-radius discovery both need infrastructure that doesn't exist yet (Location service for geo; TIERED needs per-tier timer state beyond what BROADCAST needs).
- Step 11 above (Stage-3 concurrent fan-out) — next natural step once someone picks this back up.
- Minor code-quality items from task review (not blocking): `TopOnlineDrivers(limit=0)` returns the whole pool due to Redis's `-1`-means-"last element" semantics on `ZREVRANGE` — only matters if this is ever called with `limit=0`, which nothing currently does; a couple of small DRY/dead-code nits in the Redis repository files.

---

## ride-service + driver-service: consume `ride.accepted` (2026-07-21)

Closed the gap noted in the 2026-07-19 section and in `CLAUDE.md`'s "Matching algorithm status": matching-service was publishing `ride.accepted` to no one.

- `ride-service`: new `internal/consumers/ride_accepted_consumer.go` (same reader-loop shape as matching-service's own consumers, `GroupID: "ride-service"`) decodes the event and calls a new `MarkRideMatched` command (`internal/application/command/mark_ride_matched.go`), which does a single guarded `UPDATE ride.ride SET driver_id, status='Matched', matched_at WHERE id=$1 AND status='Requested'` (`internal/persistence/ride_postgres_repository.go`) — the `status='Requested'` guard makes it idempotent against at-most-once redelivery. `domain.RideRepository` gained the `MarkRideMatched` method; wired into `app.Commands` and started in `cmd/main.go` alongside the existing outbox worker.
- `driver-service`: new `internal/consumers/ride_accepted_consumer.go` (`GroupID: "driver-service"`) calls a new `ProcessRideAccepted` command (`internal/application/command/process_ride_accepted.go`) — currently a **placeholder that only logs**, since there's no persisted "driver is on a ride" state today (`driver_profile.status` CHECK only allows `Offline`/`Online`, and no `ride.completed`/`ride.cancelled` event exists yet to ever flip it back). The seam (consumer → command) is in place so real logic has somewhere to go once that flow exists.
- Also discovered while investigating: `ride-service` was already fully Stage-2 (CQRS/DDD layered) — `CLAUDE.md`/`PLAN.md`'s "Stage 1, all in `cmd/main.go`" description was stale, just missing a `consumers` package. Corrected in the status table above.

Follow-up not done yet: extending `services/e2e-test`'s ride actor to assert a ride flips to `Matched` after an accept (per CLAUDE.md's guidance to keep the simulator covering new behavior) — flagged, not started.

---

## ride-service, matching-service, driver-service: ride cancellation (2026-07-23)

Full cancellation flow shipped end-to-end, closing the `ride.cancelled` gap flagged in the 2026-07-19 section above:

- `ride-service`: new `DELETE /ride/{id}` → `RideHandler.Cancel` → `CancelRideHandler` (`internal/application/command/cancel_ride.go`). Guards: 403 if the caller (`X-User-Id`) doesn't own the ride, 409 if the ride is already `Completed`/`Cancelled`. Computes a cancellation fee via a new port, `services.CancellationFeeCalculator` — currently only `infrastructure/fee.StubCalculator`, which always returns 0 (real fee logic needs the not-yet-built Billing service, so the field is wired up but inert). Publishes `ride.cancelled` through the existing outbox in the same transaction as the DB update. Schema gained `ride.ride.cancelled_at`/`cancellation_reason` (`services/shared/migrations/init.sql`). New contracts: `CancelRideRequest`/`CancelRideResponse` (`contracts/http`), `RideCancelledEvent` (`contracts/kafka`).
- `matching-service`: new `RideCancelledConsumer` → `CancelRideHandler` (`internal/application/command/cancel_ride.go`). Clears `ride:{id}:offered_drivers`/`pending_ride:{id}`, sets `ride:{id}:cancelled` (previously checked on accept but never set by anything), and — if the ride had already been matched — restores the driver to the `drivers:online` ZSET. Deliberately reads its **own** Redis `AcceptedBy` as the source of truth for which driver to restore rather than the `DriverID` field on ride-service's event: that field reflects ride-service's Postgres row, which can lag matching-service's own Redis state (updated the instant `AcceptRideHandler` runs, well before ride-service consumes `ride.accepted`).
- `driver-service`: new `RideCancelledConsumer` → `ProcessRideCancelledHandler` (`internal/application/command/process_ride_cancelled.go`) — **placeholder only**, same as the existing `ProcessRideAccepted` handler from the 2026-07-21 section: logs and returns, because there's still no persisted "driver is on a ride" state (`driver_profile.status` CHECK only allows `Offline`/`Online`). Both placeholders share the same follow-up: add real on-ride state and this handler has something to reverse.
- `services/e2e-test`: `client_actor.go` now runs a full cancel lifecycle every 3rd iteration (`cancelAndVerifyRide`): non-owner cancel → expect 403, owner cancel → expect `Cancelled`, re-read the ride → confirm `Cancelled`, repeat cancel → expect 409.

~~Follow-up not done yet: driver-service's persisted on-ride state (would turn both `ProcessRideAccepted` and `ProcessRideCancelled` from placeholders into real handlers) — flagged, not started.~~ Done, see below.

---

## driver-service: persisted on-ride status (2026-07-23)

Closed the follow-up flagged above: `driver.driver_profile.status` gains a third value, `'OnRide'` (CHECK constraint in `services/shared/migrations/init.sql` — note this only affects fresh DBs, since there's no migration tool; existing local Postgres volumes need `docker-compose down -v` or a manual `ALTER TABLE ... DROP/ADD CONSTRAINT`).

- `domain.DriverProfileRepository` gained `UpdateDriverStatus(ctx, id, fromStatus, toStatus string) (bool, error)` — guarded by the expected *current* status (same idempotency idiom as `ride-service`'s `MarkRideMatched`), so an at-most-once Kafka redelivery is a harmless no-op (`bool` false) rather than an error. Implemented in `persistence/driver_profile_postges_repository.go` via the tx-aware `Executor(ctx, r.db)` helper.
- `ProcessRideAcceptedHandler` (`internal/application/command/process_ride_accepted.go`) and `ProcessRideCancelledHandler` (`process_ride_cancelled.go`) are no longer log-only placeholders: they now wrap the status flip in `TransactionManager.WithinTransaction` and call `UpdateDriverStatus` — `Online → OnRide` on `ride.accepted`, `OnRide → Online` on `ride.cancelled` (no-op if the event's `DriverID` is nil, i.e. cancelled pre-match). Both constructors now take `profileRepo`/`transaction`, wired in `cmd/main.go`.
- Deliberate design choice: a `ride.cancelled` event does **not** force a driver back to `Online` if they aren't currently `OnRide` (e.g. they already ended their shift mid-ride) — that would silently re-enter them into matching-service's `drivers:online` pool without consent.
- No new outbox/Kafka event is published — this is a pure internal status flip; matching-service already manages `drivers:online` independently via its own accept/cancel handlers.
- `services/e2e-test`: `driver_actor.go`'s `acceptOffer` now polls (`verifyOnRide`, new op `driver.profile.onride`) for the driver's profile to report `OnRide` after a successful accept, retrying a few times since the flip is async over Kafka.

~~Scope was deliberately kept to driver-side status only — ride-service's own `status` stays at `Matched` (no `InProgress`/`Completed` transition yet). That's real remaining work, but needs a driver-initiated-action auth model that doesn't exist anywhere in this repo yet (no service-to-service HTTP calls exist today), so it deserves its own design pass — not started.~~ Done, see below.

---

## ride-service, driver-service: ride lifecycle `Matched` → `InProgress` → `Completed` (2026-07-23)

Closed the follow-up flagged above: ride-service's own `status` now advances past `Matched`. No migration needed — `InProgress`/`Completed` and `started_at`/`finished_at` already existed in `ride.ride`'s schema, unused until now.

- Two new driver-triggered HTTP endpoints on ride-service: `POST /ride/{id}/start` and `POST /ride/{id}/complete` (`internal/interfaces/http/handler/ride_handler.go` → `StartRideHandler`/`CompleteRideHandler`, `internal/application/command/start_ride.go`/`complete_ride.go`). Both follow `CancelRideHandler`'s exact shape: `GetRideForUpdate` inside `WithinTransaction` to lock+validate, an unconditional repo write (`MarkRideStarted`/`CompleteRide` in `persistence/ride_postgres_repository.go`), then an outbox-published event (`ride.started`/`ride.completed`, new `RideStartedEvent`/`RideCompletedEvent` in `contracts/kafka`).
- **Driver auth**: since no driver-authenticated HTTP pattern exists anywhere in this repo (confirmed: matching-service's accept endpoint and every driver-service endpoint trust a self-asserted `driverId` with zero header/auth check), the new endpoints take `driverId` in the request body (`StartRideRequest`/`CompleteRideRequest` in `contracts/http`) and validate it against the ride's stored `driver_id`, returning 403 on mismatch — still self-asserted, but free correctness value.
- Status guards are explicit allow-lists (`Matched`→`InProgress`, `InProgress`→`Completed`), not deny-lists like `CancelRide` — fewer states are startable/completable than are cancellable. No redundant `WHERE status=...` guard on the repo writes themselves (unlike `MarkRideMatched`'s self-guarded Kafka-consumer pattern) — the transaction-locked `GetRideForUpdate` + command-layer validation already serializes and checks this, and a redundant guard would silently mask a handler bug instead of surfacing it.
- `cancel_ride.go`'s terminal-state guard now also blocks `InProgress` (409) — a rider shouldn't be able to cancel a ride the driver has already started.
- driver-service reacts to `ride.completed` (new `RideCompletedConsumer`/`ProcessRideCompletedHandler`, mirroring the `ride.cancelled` consumer): flips `OnRide → Online` via the existing `UpdateDriverStatus`, and — only if that flip actually happened (`bool` true) — increments `driver_profile.total_rides_completed` via a new `IncrementRidesCompleted` repo method. Reusing `UpdateDriverStatus`'s guard result as the redelivery-idempotency signal means a duplicate `ride.completed` can't double-count a ride, with no new dedup mechanism needed. `ride.started` is published for symmetry with the rest of the ride-event lifecycle but has no consumer yet — a deliberate, acknowledged bit of dead-for-now infrastructure.
- `services/e2e-test`: `driver_actor.go`'s `acceptOffer` now runs the full cycle after accepting — `verifyOnRide` (returns a `total_rides_completed` baseline) → `startAndVerifyRide` (op `ride.start`) → simulated driving delay → `completeAndVerifyRide` (op `ride.complete`) → `verifyBackOnline` (op `driver.profile.backonline`, polls for `Online` + the increment). New `RideClient.StartRide`/`CompleteRide` in `apiclient/ride.go`.

Follow-up not done yet: `client_actor.go`'s cancel test only exercises pre-match `Requested` rides — a scenario asserting 409 when cancelling a driver-started `InProgress` ride would be a good addition, not built here.

---

## auth-service: Stage 2 refactor + `/me`/`/logout` + docs sync for the API Gateway (2026-07-25)

- `auth-service` moved to Stage 2, mirroring `ride-service`'s layering (`domain`/`application`/`persistence`/`interfaces`/`infrastructure`, decorator-wrapped CQRS handlers) but deliberately **without** an outbox — auth publishes no events and every write is a single statement. JWT/bcrypt logic moved behind `application/services` ports (`PasswordHasher`, `TokenIssuer`) with adapters in `infrastructure/security`; `cmd/main.go` is now a thin composition root.
- New: `GET /me` (caller's own profile, from `X-User-Id`) and `POST /logout` (revokes one refresh token, scoped to the caller). `auth.user` gained `updated_at`/`deleted_at` (soft-delete, read-filtered everywhere; nothing sets `deleted_at` yet — forward-looking, like driver-service's outbox once was before it was wired up).
- Two new protected Kong routes added for `/me`/`/logout` (`gateway/kong.yml`), mirroring the existing `auth-service-users` route.
- Found while building the container image: `services/auth-service/Dockerfile`'s builder stage still only did `COPY services/auth-service/cmd ./cmd` — a leftover from the Stage-1 single-file build. Adding `internal/` packages broke the image build immediately (`package auth-service/internal/... is not in std`). Fixed to `COPY services/auth-service .`, matching every other Stage-2 service's Dockerfile.
- **Docs sync, not new work**: the API Gateway (Kong, `gateway/kong.yml`) was built in the prior session but never documented in `CLAUDE.md`/`PLAN.md` — added an "API Gateway (Kong)" section to `CLAUDE.md` and corrected the "API Gateway... planned but not scaffolded" line. Also corrected `CLAUDE.md`'s stale claim that ride-service was still Stage 1 (`PLAN.md`'s own status table already had this right as of 2026-07-21; `CLAUDE.md` was never updated to match).
- Verified end-to-end through Kong on a fresh `docker-compose up --build` (fresh Postgres volume, so `init.sql`'s new columns applied automatically): signup → login → `/me` → `/users` → bad-password (401) → bad-refresh (401) → `/logout` → refresh-with-revoked-token (401). Ran `services/e2e-test` against the live stack (`auth.me`, `auth.logout`, `auth.logout.refresh_rejected` all green).

## Role-table refactor: `auth.user` as identity, `auth.client`/`auth.admin`/`driver.driver` as roles (2026-07-25)

Fixed a data-model inconsistency: `ride.ride.client_id` was `auth.user(id)` (a user id) while `ride.ride.driver_id` was `driver.driver_profile(id)` (a profile id) — two different modelling philosophies for the two ride parties. Also added the missing `Admin` role, since nothing gated the read-only dashboard before this.

- [x] `init.sql`: `auth.user.role` CHECK gains `Admin`; new `auth.client`/`auth.admin` (`id UUID PK`, `user_id UUID UNIQUE FK`); `driver.driver_profile` renamed to `driver.driver`, `name`/`phone` columns dropped (auth.user is the single source now); `ride.ride.client_id` repointed at `auth.client(id)`; `ride.ride.driver_id` gets its first real FK (`driver.driver(id)`, added at the bottom of the file since the driver schema is created after ride); one admin seeded (`admin@myubergo.local` / `admin123` locally)
- [x] `contracts/http`: `RoleAdmin`; `UserDto.ClientId` (populated by `GET /me` only); `DriverProfileDto`/`CreateDriverProfileDto`/`UpdateDriverProfileDto`/`CreateDriverProfileResponse` → `DriverDto`/`CreateDriverDto`/`UpdateDriverDto`/`CreateDriverResponse`, name/phone dropped
- [x] auth-service: gained its first `TransactionManager` (copied ride-service's `persistence` pattern) so `Signup` writes `auth.user` + `auth.client` atomically for a `Client`; `NewUser` still rejects `Admin`; `TokenIssuer.IssueAccess` gains a `clientID` param → `client_id` JWT claim (empty for non-Client); `Login`/`Refresh` look up the client row via a new `ClientRepository`; `GET /me` populates `UserDto.ClientId`
- [x] `gateway/kong.yml`: `inject_user_headers` also sets `X-Client-Id` from the `client_id` claim; new `&require_admin` Lua snippet (403 unless `X-User-Role == "Admin"`); three new path+method-matched Admin-only routes (`GET /api/ride`, `GET /api/driver/driver`, `GET /api/driver/driver-shift`) that Kong's router ranks above the broader protected routes; `/api/auth/users` gains the same gate
- [x] ride-service: `POST /request-ride`/`DELETE /ride/{id}` read `X-Client-Id` instead of `X-User-Id`
- [x] driver-service: `DriverProfile`→`Driver` renamed end-to-end (domain, repository, persistence — also fixed the pre-existing `postges` filename typo, application commands/queries, HTTP handler, `cmd/main.go` routes `/driver-profile`→`/driver`); `DriverProfileSortColumns`→`DriverSortColumns` drops the removed `driverName`/`phone` sort keys
- [x] `web/`: `DriverDto` (drops name/phone, so the Drivers table shows `userId` instead of a name); `UserDto.role` gains `'Admin'`; login-gate now surfaces a distinct "restricted to Admin accounts" message on a 403 from any list endpoint, since a non-Admin can authenticate but every table now rejects them
- [x] `services/e2e-test`: `apiclient`/actors renamed to match; simulator logs in once at startup as the seeded admin (`Deps.AdminAccessToken`) and every list-endpoint verify (`auth.users.list`, `driver.shift.list`, `ride.list`) now uses that token instead of the actor's own; `account.clientID` captured from `GET /me` after login and used for ride `clientId` assertions instead of `userID`; driver's phone-update verify repurposed to a licence-plate update, since `driver.driver` no longer has a phone field
- [x] Bruno collection: `Driver Service/*Driver Profile*.bru` → `*Driver*.bru`, paths updated to `/api/driver/driver`, bodies drop name/phone; new `Admin Log in` request; the three now-admin-gated list requests use a new `{{admin_auth_token}}` env var
- [x] `go build`/`go vet`/`go test` clean on all 4 services; `web`'s `tsc --noEmit` clean

---

## Money representation refactor + billing-service (2026-07-25)

Built `billing-service` per `docs/billing/BILLING_SPEC.md` — a Stage-2 service that turns
completed/fee-bearing rides into invoices, collects them through a pluggable payment provider (a
stub for now), and records every money movement in a double-entry ledger. Real Stripe integration
is out of scope for this pass; see the spec's §9 for the deferred list.

**Step 1 — money as integer minor units + currency, repo-wide** (own commit, gate before any
billing work): `ride.tariff`/`ride.ride`/`driver.shift` money columns renamed to `*_minor BIGINT` +
`currency CHAR(3)` (rename, not retype, so the compiler finds every call site); a `"Standard USD"`
tariff seeded alongside the four EUR ones so multi-currency is exercisable from real traffic, not
just unit tests. `RideRequestedEvent`/`RideCompletedEvent`/`RideCancelledEvent` all gained a
**`ClientID`** field — missing from the original spec draft, and load-bearing: without it
billing-service cannot know whom to invoice, and there's no sync service-to-service HTTP path to
look it up (D2 in the spec forbids introducing one). `ride-service` gained real tariff-driven fare
calculation (was hardcoded `distanceKm=10.0, price=10.0`): haversine distance between pickup/dest,
`fare = base + per_km*distance + per_min*duration`, all in minor units, rounded once at the end per
the spec's rounding rule. `CreateRideRequest` gained an optional `tariffName`. matching-service also
touched (`domain.Ride.PriceMinor`, Redis hash fields, `DriverOfferDto`) — the original spec draft
missed that it persists `RideRequestedEvent.Price` too. `driver.shift.total_earnings` renamed to
`total_earnings_minor` but deliberately **not** accumulated into — the authoritative driver-earnings
number is the ledger's `driver_payable` balance; duplicating it into a mutable shift column would be
exactly the stored-balance anti-pattern the ledger's own invariants forbid. `ride-service`'s
`CancellationFeeCalculator` port signature changed to `(int64, string, error)`; its stub now returns
a flat 300-minor-unit fee when a driver was already assigned (was always 0, permanently dead) so the
cancellation-fee billing path is actually exercisable.

**Step 2 — `billing` schema**: `customer`, `payment_method` (partial unique index: one active
default per client), `invoice` (**`UNIQUE (ride_id, type)`** — the sole idempotency guard against a
redelivered Kafka event), `invoice_line`, `payment` (deterministic `idempotency_key`,
`invoice:{id}:attempt:{n}`), `ledger_account`/`ledger_transaction`/`ledger_entry` (append-only), and
`outbox_message`. One correction to the spec's own schema: a plain `UNIQUE(type, owner_id, currency)`
on `ledger_account` does **not** collapse platform-level accounts (`owner_id IS NULL`) to one row per
currency, since Postgres treats every NULL as distinct in a unique index — replaced with a
`COALESCE(owner_id, sentinel-uuid)` expression index.

**Steps 3–9 — service + pipeline**: `services/billing-service`, Stage 2 from day one, copying
ride-service's layering and every shared piece (decorator/errors/reqctx, persistence helpers,
outbox worker, health/kafka/metrics/shutdown infra) by copy, matching this repo's existing
no-shared-runtime-module convention. `PaymentProvider` port defined in its own vocabulary
(`ChargeResult{Status, ProviderIntentID, FailureCode, FailureMessage}`), not mirroring Stripe's shape
— the entire reason a real Stripe adapter stays additive later instead of a domain refactor.
`StubProvider` picks outcomes from the payment method token (`pm_stub_ok`/`pm_stub_decline`/
`pm_stub_insufficient`), caches by idempotency key. `ride.completed`/`ride.cancelled` consumers →
`CreateInvoiceFromRide` command posting **T1** (`invoice_opened`: client_receivable debited;
platform_revenue + driver_payable credited, split by `PLATFORM_COMMISSION_BPS` via truncating
integer division so the two always sum exactly to the fare — no `driver_payable` leg for a
cancellation fee, 100% to `platform_revenue`). A `ChargeWorker` (ticker+select, same shape as
`OutboxWorker`) sweeps invoices due for collection, posts **T2** (`payment_succeeded`) on success or
retries with backoff (`PAYMENT_BACKOFF`, default `1m,5m,30m`) up to `MAX_PAYMENT_ATTEMPTS` (default
3) before posting **T3** (`invoice_uncollectible`) — `driver_payable` deliberately stays posted from
T1 even here, since the driver is still owed money the platform never collected. Not in the original
spec, needed: a client with no active payment method fails with `no_payment_method`, counting toward
`attempt_count` like any other decline. `ride-service` gained a `payment.completed` consumer +
`MarkRideBilled` command setting `ride.ride.bill_id`, closing a loop the README had documented as
target-only. Kong gained `billing-service` (caller-scoped, ownership checked in-service against
`X-Client-Id` since Kong has no concept of invoice ownership), `billing-service-admin-list`
(`GET /invoices`, Admin-only), and `billing-service-ledger-balance` (`GET /ledger/balance`,
Admin-only — the cheapest possible regression check for the ledger invariants), all following the
existing paths+methods-ranking trick.

**Real bug found and fixed during manual verification** (not caught by unit tests, since it only
manifests against a real Postgres transaction): the idempotency no-op path was broken. On a
`(ride_id, type)` unique-violation, `CreateInvoiceFromRideHandler.Handle` logged and returned `nil`
from *inside* the `WithinTransaction` closure — but a real SQL error aborts the Postgres transaction
server-side regardless of whether the Go code "handles" it, so `WithinTransaction`'s subsequent
`tx.Commit()` itself failed with `"current transaction is aborted"`, turning the intended no-op into
a real error every time. Fixed by returning the error from the closure (so `WithinTransaction` rolls
back — always safe, even on an already-aborted transaction) and only translating
`ErrDuplicateInvoice` into a successful `nil` *after* `WithinTransaction` returns. Verified via a
manual Kafka replay of a `ride.completed` message: before the fix, `commands.createinvoicefromride`
recorded `.failure`; after, it recorded `.success` with the "no-op" log line and no duplicate ledger
transaction.

**e2e-test**: `apiclient/billing.go`; every `ClientActor` attaches a payment method right after
signup (`billing.paymentmethod.add`/`.list`), occasionally requests against the `"Standard USD"`
tariff; a dedicated `decline-client-0` always uses `pm_stub_decline`; `DriverActor` polls
`GET /rides/{rideId}/invoice` after completing a ride for a terminal `paid`/`uncollectible` status
(`billing.invoice.get`) and then exercises `GET /ledger/balance` (`billing.ledger.balance`) as a
cheap end-to-end ledger-query smoke test. Bruno gained a `Billing Service` folder; `web/` gained an
Admin-only Invoices page and a `formatMoney` helper (money is formatted at the display edge only,
never divided in component state).

**Verified**: `go build`/`go vet`/`go test` clean on all 5 services (including new ledger-invariant
and fare-split unit tests in billing-service) and e2e-test; `web`'s `tsc --noEmit` clean; a fresh
`docker-compose down -v && up --build` applies the new schema cleanly; a live `services/e2e-test` run
against Kong showed 0 http/verify failures across every billing op; manual verification confirmed
the idempotency guard (Kafka replay → no duplicate invoice/ledger transaction after the fix above),
the full decline→uncollectible→T3 path (with `driver_payable` correctly still posted), and Kong's
auth boundary (`GET /api/billing/invoices` 403s a Client token and 200s the seeded admin; a Client
reading another client's invoice 403s in-service).

**Deferred, per the spec**: driver payouts/Connect, client wallet/credits/promos/refunds, pre-ride
payment-method validation, FX conversion, receipt rendering, and a reconciliation poller for
payments stuck in `processing` (real Stripe adapter/webhooks landed 2026-08-01 — see below, and
that poller is the next open item on this list, now unblocked).
See `docs/billing/BILLING_SPEC.md` §9 for the full list and why each stays additive.

---

## billing-service: real Stripe adapter + webhooks; ChargeWorker retry-spam fix (2026-08-01)

Closed the first two items on the spec's §9 deferred list — a real, test-mode-only Stripe adapter
now exists alongside the original stub, selected by `PAYMENT_PROVIDER=stub|stripe`:

- `StripeProvider` (`internal/infrastructure/payment/stripe/stripe_provider.go`) implements
  `PaymentProvider`/`CustomerVault`/`ProviderEventParser` against `stripe-go/v84`.
  `EnsureCustomer` get-or-creates a Stripe customer per client, idempotent via a deterministic
  `customer:{clientID}` key. `AttachPaymentMethod` attaches a payment-method token to that
  customer and returns Stripe's own brand/last4/expiry (never the caller's self-asserted claim).
  `Charge` creates an off-session, immediately-confirmed `PaymentIntent`, reusing the existing
  `invoice:{id}:attempt:{n}` idempotency key as Stripe's own `Idempotency-Key` header — the same
  crash-safety guarantee the stub already had, now backed by a real provider. `NewStripeProvider`
  refuses to start on any secret key not prefixed `sk_test_`, keeping this sandbox-only, permanently,
  enforced in code rather than only in docs.
- `WebhookHandler` (`internal/interfaces/http/handler/webhook_handler.go`) + a new `billing.psp_event`
  inbox table (`internal/domain/psp_event.go`, `internal/persistence/psp_event_postgres_repository.go`)
  handle Stripe's async `payment_intent.succeeded`/`.payment_failed`/`.processing`/`.canceled`/
  `.requires_action` callbacks: signature verified over the raw body before any decode, deduped by
  Stripe's own event id, and dispatched through the exact same `FinalizeChargeSucceeded`/
  `FinalizeChargeFailed` commands `ChargeWorker`'s synchronous path already used — so a race between
  an instant answer and a delayed webhook can never double-post the ledger (the same guarded-update
  idiom as every other finalize path in this service). `mapStripeOutcome` (`stripe_provider.go`) is
  shared between the synchronous `Charge()` response and the webhook path, since a card decline on a
  synchronously-confirmed `PaymentIntent` arrives from the Stripe SDK as an API error, not a 200 with
  a failed status — one function normalizes both shapes so callers never need to know which occurred.
- Kong already had a public (no jwt), path-matched route for `POST /api/billing/webhooks/stripe` from
  a prior pass — no gateway changes needed here.
- `services/e2e-test` already picks provider-aware fixture tokens (`cmd/main.go:78-85`):
  `pm_card_visa`/`pm_card_chargeCustomerFail` under `PAYMENT_PROVIDER=stripe`, vs
  `pm_stub_ok`/`pm_stub_decline` otherwise — no e2e-test code changes needed, just matching env vars
  in its own shell (it's run manually, not in docker-compose).

**Real bug found and fixed while verifying this end-to-end** (found by an actual docker-compose +
e2e-test run, not a unit test): `ChargeWorker`'s "client has no active default payment method"
branch (`internal/workers/charge_worker.go`) created the `billing.payment` row already in a terminal
`Failed` status, then unconditionally called `FinalizeChargeFailed`. That handler's guarded
`MarkFailed` UPDATE only matches a row still `pending`/`processing` — since the row was already
`failed`, the guard permanently no-op'd, logging `"payment already resolved, skipping"` and returning
before ever reaching the backoff/`uncollectible`/ledger/outbox logic. Because `next_attempt_at` was
therefore never advanced, the same invoice was re-selected and re-failed on every single
`ChargeWorker` tick (5s), forever — an infinite retry loop, independent of stub vs. Stripe. Fixed by
creating that row `Pending` instead, so `FinalizeChargeFailed` can actually transition it through the
same lifecycle as any other failed attempt; `buildClaim`'s resume path was also hardened for a
resumed no-payment-method row instead of hard-erroring on the (no longer valid) assumption that it
"can't happen." Covered by two new tests in `charge_worker_test.go`
(`TestChargeWorker_NoPaymentMethod_DoesNotRetryEveryTick`,
`TestChargeWorker_NoPaymentMethod_ExhaustedAttempts_MarksUncollectible`) — verified to fail against
the pre-fix code and pass against the fix.

**Verified**: `go build`/`go vet`/`go test ./...` clean in `services/billing-service`; a real Stripe
test-mode charge (via a live docker-compose stack with `PAYMENT_PROVIDER=stripe`) confirmed
end-to-end — the `PaymentIntent` appears in the Stripe dashboard and the corresponding invoice/
payment rows show `paid` in Postgres.

**Still deferred**: a reconciliation poller for payments stuck in `processing` (unblocked now that a
real async provider exists, but not built this pass), PSP-fee capture, driver payouts/Connect,
wallet/credits/refunds/FX. See `docs/billing/BILLING_SPEC.md` §9.

## Later (not detailed yet — revisit after matching-service)

- ~~**auth-service → Stage 2**: same layering as driver-service/ride-service; move JWT/bcrypt logic into an application/domain service.~~ Done, see the 2026-07-25 section below.
- **Cross-cutting Stage 3**: OpenAPI spec + codegen for HTTP contracts (replacing hand-written `contracts/http` structs), one gRPC call as a learning exercise (e.g. matching-service → driver-service for available drivers, alongside/instead of direct Postgres/HTTP access), a real Prometheus metrics client (replacing the logging stub), a real liveness probe for driver-service's health checker.
- **Phase 3 of README's product roadmap**: Location, Billing, Notification services — new services, start each at Stage 1 like the others did. (The API Gateway is done — Kong, see `CLAUDE.md`'s "API Gateway (Kong)" section and the 2026-07-25 section below.)

## Tech Debt audit + remediation roadmap (2026-08-01)

Full audit (~150 findings across code-quality, Go-concurrency-idiom usage, and dependency/infra/CI) run across all 5 services. Two real bugs were confirmed by reading source before fixing. Phased plan below; Phase 0 landed this pass, Phases 1-3 are open. This section is additive to — not a replacement for — item 11 in "C. Stage 3 preview" above and the "Cross-cutting Stage 3" bullet above; both are cross-referenced rather than duplicated.

### Phase 0 — bug fixes & security gaps (done, 2026-08-01)

- [x] **Readiness probe never updates after startup** — `internal/infrastructure/health/health.go` in all 5 services called `defer ticker.Stop()` inside `Start()`, which fires when `Start()` returns (immediately after launching the background goroutine), not when the goroutine exits. `/health/ready` was computed once at boot and never refreshed — a DB/Redis outage after startup never flipped it to unready. Fixed by moving ticker creation/`Stop()` inside the goroutine itself in all 5 services.
- [x] **Invoice ownership bypass** — `billing-service`'s `InvoiceHandler.GetByID`/`GetByRideID` only checked `if clientID != "" && inv.ClientID != clientID`, so a caller with no `X-Client-Id` (any Driver or Admin-role token, since only Client tokens carry that claim) skipped the check entirely and could read any invoice by ID. Fixed to deny (403) whenever `clientID == ""`. (The payment-method handlers already rejected an empty `X-Client-Id` correctly — only the invoice handler had this bug.)
- [x] **Postgres/Redis bypassed the Kong-only-ingress design** — `docker-compose.yml` published `postgres:5432` and `redis:6379` to the host with no documented reason (unlike matching-service's `:8002`, which is an intentional, documented exception — see below). Removed both host-port publishes; use `docker compose exec` for local inspection.
  - Note: matching-service's `:8002` host port was **not** removed, despite being flagged in the audit — CLAUDE.md documents it as a deliberate exception (no Kong route exists for it, and the e2e-test simulator's driver actors poll `GET /drivers/{driverId}/offer` directly against it). Closing that gap properly means adding a Kong route for matching-service, not just dropping the port; tracked as a Phase 1/2 candidate, not done blind in Phase 0.
- [x] **No request body size limit** — added a `BodyLimit` middleware (1MB cap via `http.MaxBytesReader`) to all 5 services, chained outermost around the existing `RequestID` middleware in each `cmd/main.go`.
- [x] **JWT parser didn't pin accepted signing method** — `auth-service`'s `ParseRefresh` (`internal/infrastructure/security/jwt_issuer.go`) now passes `jwt.WithValidMethods([]string{"HS256"})` to guard against algorithm-confusion attacks.
- [x] **No email/password validation on signup** — `domain.NewUser` now checks email shape via a permissive regex; `SignupHandler.Handle` now rejects passwords under 8 characters before hashing (the only place with access to the raw password — `NewUser` only ever sees the hash).
- [x] **driver-service `Update` handlers ignored JSON decode errors** — `driver_handler.go`/`shift_handler.go` `Update` now check and surface the decode error (400) instead of discarding it and proceeding with a zero-value request.
- [x] **driver-service `GetByID` collapsed every error to 404** — both handlers now distinguish `commonerrors.ErrNotFound` (404) from other errors (500), matching the pattern already used by `Update` and `billing-service`'s `InvoiceHandler`.

Verified: `go build ./... && go vet ./... && go test ./...` clean in all 5 services after the above.

### Phase 1 — foundational safety nets (mostly done, 2026-08-01)

- [x] **CI**: `.github/workflows/ci.yml` — matrix job per Go service (`go build ./...`, `go vet ./...`, `go test ./...`, `golangci-lint run`) + a `web` job (`npm ci`, `oxlint`, `npm run build`). Root `.golangci.yml` (errcheck/govet/ineffassign/staticcheck/unused) verified to pass clean (`0 issues`) against all 5 services by actually installing and running `golangci-lint` locally, not just written blind — one real pre-existing dead-code hit (`fakeLedgerRepo.count()`, an unused test helper in billing-service) was found and deleted in the process.
- [x] **Secret hygiene**: `docker-compose.yml`'s `PG_DSN`/`JWT_SECRET`/Postgres creds now interpolate from `${VAR:-default}` (defaults unchanged, so local dev behavior is identical); `.env.example` documents `JWT_SECRET`/`POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB`/`APP_ENV`. Each service with a real secret (auth/ride/driver/billing's `PG_DSN`; auth's `JWT_SECRET`) now refuses to start (`log.Fatal`) if `APP_ENV=production` and the value is still the documented default — opt-in, so local dev (`APP_ENV` unset) is unaffected. `gateway/kong.yml`'s existing comment already covered the manual-sync caveat, so left as-is.
- [x] **Panic recovery**: added a `Recover(logger)` HTTP middleware (chained innermost, around `mux`) to all 5 services, plus a `goSafe(logger, name, fn)` helper in each `cmd/main.go` wrapping every long-lived worker/consumer/HTTP-server goroutine (~20 sites total) with `recover()` + structured logging, so one bad message/panic logs and dies instead of taking the whole process down.
- [x] **Context timeouts on consumer-triggered work**: all 10 Kafka consumer files (ride/driver/billing/matching) now wrap their command-handling call in `context.WithTimeout(ctx, 10*time.Second)` instead of passing the raw unbounded consumer context through.
- [x] **Outbox dead-letter / retry cap**: added `defaultMaxRetries = 10` to all 3 outbox workers (ride/driver/billing) — a message that's failed 10 times is skipped every subsequent tick (logged at Error) instead of retried forever, and stays visible via `SELECT * FROM outbox_message WHERE NOT processed AND retries >= 10` for manual triage. Deliberately no schema change (no new `dead_lettered` column) — the existing `retries` column already carries enough signal, and this keeps the fix a pure worker-side change.
- [x] **docker-compose hardening**: `restart: unless-stopped` + `mem_limit` (+ `cpus: 0.5` on the 5 Go services) added to every service; Kafka gained a real `healthcheck:` (`kafka-broker-api-versions`) since it previously had none — everything that depended on Kafka now waits on `condition: service_healthy` instead of `service_started`. Each Go service gained a `healthcheck:` backed by a new `app healthcheck` subcommand (checked first thing in `main()`, before any DB/Kafka setup) that does a plain HTTP GET against its own `/health/ready` and exits 0/1 — distroless has no shell/curl for a `CMD-SHELL` check, and `/health/live` was deliberately not used since it's still hardcoded `true` (a separate, tracked Phase 3 item) and would make the healthcheck meaningless. Kong's `depends_on` for the 4 fronted services upgraded from `service_started` to `service_healthy` accordingly, closing the "Kong can start routing before a dependency is actually ready" race.
- [x] **Error responses**: added a `writeInternalError(w, err)` helper next to each service's `writeError` (auth/ride/driver/billing) that logs the real error server-side and returns a generic `"internal server error"` message; replaced all 22 `writeError(w, err.Error(), http.StatusInternalServerError)` call sites across those 4 services.
- [ ] **Migration tooling**: deliberately deferred, not attempted this pass — adopting `golang-migrate`/`goose` and splitting the existing 373-line/44-object `init.sql` is a real design decision (which tool, how to handle the already-decided "one baseline file" convention going forward) rather than a mechanical fix, and risks getting it wrong without dedicated review. Revisit as its own pass.
- [ ] **Kong route for matching-service**: deliberately deferred — matching-service's exposed `:8002` is documented in CLAUDE.md as an intentional exception (no Kong route exists for it, and the e2e-test simulator's driver actors poll `GET /drivers/{driverId}/offer` directly against it), not an oversight like the Postgres/Redis ports were. Adding a route changes matching-service's trust model (it does zero internal auth today, unlike the other 4 services which trust Kong-injected headers) and needs its own design pass, not a blind edit.

### Phase 2 — Go concurrency/idiom modernization (open; supersedes/extends item 11 above)

- [ ] **`matching-service`'s `BroadcastOffersHandler`** (`internal/application/command/broadcast_offers.go`) — this *is* item 11 in "C. Stage 3 preview" above, made concrete: today it's up to ~20 sequential, blocking one-at-a-time Redis round-trips per broadcast round (2-3 calls x 5 candidates in the filter loop, 2 calls x 5 targets in the send loop), no pipelining, no fan-out. Fan out the target-offer loop via goroutines + `errgroup.Group` (or `WaitGroup` + buffered result channel) with `select` on `ctx.Done()`; pipeline the candidate-filtering Redis calls via `redis.Client.Pipeline()` (same server, batchable — a better fit than goroutines there).
- [ ] **Outbox workers** (ride/driver/billing) — currently a single goroutine publishes sequentially inside one open DB transaction (up to ~50s worst case for a 10-message batch). Split "claim batch" from "publish," fan out publishing with a bounded `errgroup.Group.SetLimit` pool so the transaction/row-locks are held for a fraction of the time.
- [ ] **Redis client tuning**: `matching-service/cmd/main.go` constructs `redis.NewClient` with only `Addr`/`Password`/`DB` — add `PoolSize`/`DialTimeout`/`ReadTimeout`/`WriteTimeout`/`MaxRetries` (the 4 Postgres-backed services already tune their pools this way; bring Redis to parity).
- [ ] **`golang.org/x/time/rate`**: consider replacing matching-service's manual Redis-counter "3 offers/minute" limiter with `rate.Limiter` where a local (non-distributed) limiter is sufficient, or explicitly document why the distributed counter is intentionally kept.
- [ ] Add `errgroup` to `go.mod` where used above (zero uses repo-wide today).

### Phase 3 — larger, ongoing investments (open)

- [ ] **Test coverage**: every service has zero tests for `persistence`, `consumers`, and (except billing's `ChargeWorker`) `workers` packages — the layers most likely to break silently. Start with `ride-service` and `billing-service` (money-handling).
- [ ] **Dependency modernization**: `segmentio/kafka-go v0.4.28` (never reached stable v1) and `lib/pq v1.10.7` (archived upstream, community moved to `jackc/pgx`) are the highest-risk pins; also unify `sirupsen/logrus` version (matching-service is on `v1.8.1`, everyone else `v1.9.4`) and `x/sys`/`x/crypto` drift across `go.mod` files. Multi-week migration — schedule separately from Phase 1/2.
- [ ] **Real observability** (extends the "Cross-cutting Stage 3" bullet above): replace `LoggingMetricsClient` (all 5 services just log `"Metric recorded"`) with `prometheus/client_golang`, expose `/metrics`, add Prometheus+Grafana to `docker-compose.yml`; fix the hardcoded `Live: true` in health checkers to reflect a real liveness signal.
- [ ] **Codegen** (same "Cross-cutting Stage 3" bullet): generate `contracts/http` from an OpenAPI spec, or explore one gRPC service — confirmed zero codegen exists anywhere today.
