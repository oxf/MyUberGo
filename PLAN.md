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
| ride-service | 1 | request-ride/list/get + outbox-polling goroutine, all in `cmd/main.go` |
| driver-service | 2 (+ early Stage 3 features) | full CQRS/DDD layering; graceful shutdown + health checks + logging/metrics decorators + a working transactional-outbox worker already present, but metrics is a logging-only stub (no Prometheus) and health's liveness check is hardcoded `true` (never fails) — real Stage 3 work still needed there |
| matching-service | 2, in progress, most complex | partial CQRS layering (Redis-backed commands wrapped w/ decorators) but no query handlers, no Kafka producer, no HTTP layer, and `cmd/main.go` still carries dead Stage-1 code |
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
1. Delete the dead code in `cmd/main.go`: `startRideRequestedConsumer`, `handleRideRequested`, and the now-unused `db *sql.DB` / `kafkaBroker string` package vars and their `sql.Open`/`lib/pq` import.
2. Matching-service's target design (README) is **Redis-only** — no Postgres. Confirm and drop the Postgres dependency entirely once step 1 is done (no other file in this service uses `db`).
3. Fix consumer naming/behavior: rename `RideAcceptedConsumer` → something accurate (it consumes `ride.requested`) or split so a future real `ride.accepted`-consuming type isn't confused with this one. Stop hardcoding the topic inside `Run()`; use the `topic` parameter that's already passed in (it's currently accepted and ignored).
4. Add query handlers as needed once the accept endpoint (below) requires reading offer/ride state back out of Redis.

### B. Build the matching algorithm incrementally
Implement in this order, each as its own small command/handler:

5. **Discovery (simplified)** — no Location service exists yet, so there's no real geo data. Start with rating-only ranking (skip the distance component of the README's scoring formula for now; note this simplification in code comments). Query available drivers straight from `driver.driver_profile` (still the one place this service legitimately needs Postgres read access — reconsider step 2 if so, or have driver-service expose this via HTTP instead of matching-service reading its table directly).
6. **Offer state in Redis** — implement the README's target key schema: `ride:{rideId}:offered_drivers`, `ride:{rideId}:accepted_by` (TTL, NX-set), `ride:{rideId}:cancelled`, `driver:{driverId}:current_offer`.
7. **Atomic accept endpoint** — first HTTP surface in this service: `POST /rides/{rideId}/accept`. Use Redis `SET ... NX` for the atomic claim per the README's Phase 4 design; 409 on race loss, 400 on expired/cancelled.
8. **Retry with expanding attempts** — simplified version without real geo radius (e.g. widen the candidate pool or lower the rating threshold instead of expanding a radius in km); cap attempts, give up and log after the max.
9. **Rate limiting** — `driver:{driverId}:notifications:minute` via Redis `INCR`/`EXPIRE`, skip over-limit drivers when broadcasting.
10. **Publish `ride.accepted`** — first Kafka producer in this service. Needs a new `contracts.RideAcceptedEvent` struct in `services/contracts/kafka`.

### C. Stage 3 preview (once A+B work end-to-end)
11. Convert the offer broadcast (step 6/7) to use goroutines + a channel + `select`: fan out push-style "you've been offered this ride" sends to the top-N ranked drivers concurrently, and `select` between an acceptance signal and a timeout instead of blocking sequentially. This is the natural first place to apply the concurrency patterns from Stage 3.

---

## Later (not detailed yet — revisit after matching-service)

- **ride-service → Stage 2**: same domain/application/persistence/interfaces layering as driver-service; keep the existing transactional-outbox worker (it already works) but move it and the handlers into the layered structure.
- **auth-service → Stage 2**: same layering; move JWT/bcrypt logic into an application/domain service.
- **Cross-cutting Stage 3**: OpenAPI spec + codegen for HTTP contracts (replacing hand-written `contracts/http` structs), one gRPC call as a learning exercise (e.g. matching-service → driver-service for available drivers, alongside/instead of direct Postgres/HTTP access), a real Prometheus metrics client (replacing the logging stub), a real liveness probe for driver-service's health checker.
- **Phase 3 of README's product roadmap**: Location, Billing, Notification services, and the API Gateway — new services, start each at Stage 1 like the others did.
