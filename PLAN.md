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
| auth-service | 1 | signup/login/refresh, all in `cmd/main.go` |
| ride-service | 2 | full CQRS/DDD layering (`domain`/`application`/`persistence`/`interfaces`/`workers`), decorator-wrapped handlers, transactional-outbox worker for `ride.requested`, plus a `ride.accepted` consumer (see 2026-07-21 section below) |
| driver-service | 2 (+ early Stage 3 features) | full CQRS/DDD layering; graceful shutdown + health checks + logging/metrics decorators + a working transactional-outbox worker already present, but metrics is a logging-only stub (no Prometheus) and health's liveness check is hardcoded `true` (never fails) — real Stage 3 work still needed there |
| matching-service | 2, complete for its current scope | full CQRS layering, HTTP layer, Kafka producer, graceful shutdown, Redis health checks; implements a simplified version of the README's matching algorithm (rating-only ranking, BROADCAST-only, pool-widening retry) — see the 2026-07-19 section below |
| e2e-test | n/a (tooling, not a service) | continuous client-activity simulator: N virtual clients + M virtual drivers (goroutine-per-actor) drive auth/driver/ride over HTTP with deep read-back verification and periodic stats; run manually via `go run ./cmd` against the compose stack — deliberately NOT in docker-compose. See `services/e2e-test/README.md` |

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
- No `ride.cancelled` producer/consumer anywhere — `ride:{rideId}:cancelled` is checked on accept but nothing ever sets it.
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

## Later (not detailed yet — revisit after matching-service)

- **auth-service → Stage 2**: same layering as driver-service/ride-service; move JWT/bcrypt logic into an application/domain service.
- **Cross-cutting Stage 3**: OpenAPI spec + codegen for HTTP contracts (replacing hand-written `contracts/http` structs), one gRPC call as a learning exercise (e.g. matching-service → driver-service for available drivers, alongside/instead of direct Postgres/HTTP access), a real Prometheus metrics client (replacing the logging stub), a real liveness probe for driver-service's health checker.
- **Phase 3 of README's product roadmap**: Location, Billing, Notification services, and the API Gateway — new services, start each at Stage 1 like the others did.
