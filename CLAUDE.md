# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

MyUberGo is a learning project: a distributed, event-driven clone of Uber built as independent Go microservices communicating over Kafka (async) and REST/HTTP (sync), backed by PostgreSQL (per-service schemas in one instance) and Redis. It is deliberately mid-refactor — services are in different architectural stages on purpose (see "Architectural maturity" below), so don't assume every service should look like the others.

The target system has **8 services**; only **4 are implemented** (auth, ride, matching, driver — this repo's `services/` directory). Location, Billing, Notification, and API Gateway are planned but not scaffolded. The full target design — per-service responsibilities, schemas, HTTP/Kafka contracts, the matching algorithm, and the phased roadmap — lives in `README.md`; don't duplicate it here. This file only calls out where the **current code** (what you'll actually be editing) diverges from that target, so you don't assume an unbuilt table/endpoint/topic already exists.

**See `PLAN.md` for the step-by-step, checkable implementation roadmap** (what to build next, in what order). This file (CLAUDE.md) is the architectural orientation; PLAN.md is the working task list — keep them consistent but don't duplicate PLAN.md's step list here.

## Commands

There is no root build tool, Makefile, or CI config — each service is built/run independently.

```bash
# Full stack (Postgres, Redis, Kafka, Kafka UI, and all 4 services)
docker-compose up --build

# Kafka UI (inspect topics/consumer groups): http://localhost:8080
# Postgres: localhost:5432 (postgres/postgres/postgres)
```

Per-service Go workflow (run from the service directory, e.g. `services/driver-service`):

```bash
go build ./...
go vet ./...
go run ./cmd
```

Unit tests exist for paging/sorting param parsing and DTO mapping (`go test ./...` from within the service directory); there is no integration-test suite — the e2e-test simulator is the de facto integration check.

Each service is its own Go module (`module auth-service`, `module ride-service`, `module driver-service`, `module matching-service`, plus `services/contracts` and `services/shared`). They are **not** part of a single workspace — there is no root `go.work`. Each service's `go.mod` pulls in `github.com/oxf/MyUber/contracts` via a `replace ... => ../contracts` directive, so the `services/contracts` module must sit alongside each service on disk (as it does in this repo).

Dockerfiles build with the **repo root as build context** (see `docker-compose.yml`), not the service directory — e.g. `services/driver-service/Dockerfile` does `COPY services/driver-service/go.mod ...` and `COPY services/contracts /contracts` before `COPY services/driver-service .`. Keep this in mind if you touch a Dockerfile: paths are relative to repo root, not the service.

## API collection

`MyUberGo/` at the repo root is a [Bruno](https://www.usebruno.com/) collection (`.bru` files) with requests for Auth, Driver, and Ride services, plus a `Local` environment. Update it when you add/change endpoints so it stays a usable manual test client.

## Admin dashboard

`web/` at the repo root is a Vite + React + TypeScript read-only admin dashboard (Users / Drivers / Shifts / Rides tables with server-side paging + sorting via the shared `PagedResponse[T]` envelope in `contracts/http`). Deliberately **not** in docker-compose — run it manually against the running stack:

```bash
cd web
npm install
npm run dev   # http://localhost:5173
```

The Vite dev server proxies `/api/auth` → :8000, `/api/ride` → :8001, `/api/driver` → :8003 (`web/vite.config.ts`), so the Go services need no CORS headers — keep it that way. TypeScript DTOs in `web/src/api/types.ts` mirror `services/contracts/http` json tags — update them whenever contracts change. List-endpoint paging contract (all services): 1-based `page`, `pageSize` (default 20, cap 100), `sortBy` validated against a per-endpoint whitelist, `sortDir` asc|desc.

## e2e-test simulator

`services/e2e-test` is a continuous client-activity simulator, not a business service: goroutine-per-actor virtual clients and drivers drive auth/driver/ride over HTTP (reusing the `contracts/http` DTOs), deep-verify every write by reading it back, and report per-op stats. It is deliberately **not** in docker-compose — run it manually (`go run ./cmd` from `services/e2e-test`) against the running stack; see its README for config. It sits outside the 3-stage service progression (no server, no DB, no domain — just `cmd` + `internal/apiclient|actors|stats`). When you add or change a service endpoint, extend the corresponding actor/apiclient so the simulator keeps covering it.

## Architecture

### Services and ports

| Service | Port | Role |
|---|---|---|
| auth-service | 8000 | User signup/login, JWT issuance (access + refresh), refresh-token rotation |
| ride-service | 8001 | Ride requests, fare/distance estimation, ride lifecycle, publishes `ride.requested` |
| matching-service | 8002 | Consumes `ride.requested`/`shift.updated`, matches rides to available drivers |
| driver-service | 8003 | Driver profiles and shifts (login/logout, rides, earnings), publishes `shift.updated` |

`services/contracts` is a shared Go module (no `replace` of its own) holding the wire contracts every service depends on:
- `contracts/http` — REST request/response DTOs per service (`auth-service.go`, `driver-service.go`, `ride-service.go`)
- `contracts/kafka` — Kafka event payloads (`ride-service.go` has `RideRequestedEvent`; `driver-service.go` has `ShiftUpdatedEvent`)

Changing a field used across services means editing it once in `contracts` — both the producer and consumer(s) pick it up through the `replace` directive.

### Data model

A single Postgres instance hosts one schema per service (`auth`, `ride`, `driver`; `matching` is referenced by matching-service SQL but not yet defined in the shared migration — check `services/shared/migrations/init.sql` before assuming a table exists). That file is the single source of truth for schema and is mounted into the Postgres container's `docker-entrypoint-initdb.d` — there is no migration tool/versioning, just this one SQL file run on first container start.

### Transactional outbox pattern

`ride-service` and `driver-service` write domain state and an outbox row in the same DB transaction, then a background worker (`internal/workers`, ticker + `select` loop) polls `outbox_message` (`FOR UPDATE SKIP LOCKED` inside `TransactionManager.WithinTransaction`), publishes to Kafka via an `EventPublisher` port/adapter, and marks rows processed (or increments `retries` on publish failure — no dead-letter handling yet). This is how `ride.requested` and `shift.updated` events get published reliably. `matching-service` is a pure Kafka consumer (via `segmentio/kafka-go`) on the other end, reacting to those topics; it also caches driver/ride state in Redis (`internal/infrastructure/cache`).

### Architectural maturity: the 3-stage learning progression (intentional, not inconsistency)

Every service is deliberately built through 3 stages, and different services currently sit at different stages on purpose — match a service's *current* stage when extending it rather than unilaterally jumping it ahead:

- **Stage 1 — Basic/procedural**: everything in a single `cmd/main.go`. Handlers decode a request, run raw SQL directly, encode a response. No domain/application separation.
- **Stage 2 — CQRS + DDD (Clean Architecture)**: layered into `domain`/`application`(`command`+`query`)/`persistence`/`interfaces`/`infrastructure` (see "The layered pattern" below), command/query handlers wrapped with logging+metrics decorators, a `TransactionManager` for context-propagated DB transactions, graceful shutdown, and health checks.
- **Stage 3 — Production-grade advanced Go** (not started anywhere yet): idiomatic concurrency (goroutines/channels/`select` — e.g. fan-out/fan-in for broadcasting ride offers, worker pools for outbox pollers), code generation from OpenAPI (HTTP contracts) and/or gRPC (inter-service calls) instead of hand-written structs, a real metrics/tracing backend (current metrics client is a logging-only stub, not Prometheus), and a real liveness signal (current health checker hardcodes `Live: true` and only ever reflects readiness via DB ping).

| Service | Current stage | Notes |
|---|---|---|
| auth-service | Stage 1 | signup/login/refresh, all in `cmd/main.go` |
| ride-service | Stage 1 | request-ride/list/get + an outbox-polling goroutine, all in `cmd/main.go` |
| driver-service | Stage 2 (+ early Stage-3 features) | the reference Stage-2 implementation; already has graceful shutdown, health checks, logging/metrics decorators, and a working transactional-outbox worker grafted on, but metrics is a logging stub and health's liveness check is a no-op (see infra notes below) |
| matching-service | Stage 2, in progress, most complex | partial CQRS layering (commands wrapped with decorators, Redis-backed repos) but `cmd/main.go` still carries ~110 lines of dead/unreachable Stage-1 code (`startRideRequestedConsumer`/`handleRideRequested`, unused `db`/`kafkaBroker` vars); no query handlers yet; `domain/outbox.go`/`internal/application/services/transaction_manager.go`/`internal/common/errors` are scaffolded but have zero implementations or usages; no Kafka producer exists (service only consumes/caches, never publishes) |

See `PLAN.md` for the ordered, checkable steps to move each service forward. Location, Billing, and Notification services described in the README's target design don't exist in `services/` yet at all (Phase 3 of the README roadmap) — don't assume their directories, schemas, or Kafka topics are present.

### Target vs. current schema/contracts

The README's per-service target design is ahead of what's actually in `services/shared/migrations/init.sql` and `services/contracts`. Notably not yet present in the current schema/contracts even for the 4 implemented services:
- `auth.user`: no `updated_at`/`deleted_at` (soft delete) columns.
- `driver.driver_profile`: no `license_number`/`license_expiry`/`vehicle_color`; no `driver.driver_rating` table.
- `ride.ride`: no `cancelled_at`, no separate `RIDE_REQUEST` table, no time-windowed tariffs (only one flat rate is used, `ride.tariff` isn't read by the handler).
- **No `matching` schema anywhere, full stop** — not just "not yet defined": `init.sql` only declares `auth`/`ride`/`driver`, and the one place matching-service's code references `matching.ride_offer` is dead code (`handleRideRequested` in `cmd/main.go`) that's never called from `main()`. Don't add code that assumes this table exists.
- `services/contracts/kafka` only defines `RideRequestedEvent`/`ShiftUpdatedEvent` — `ride.cancelled`, `ride.started`, `ride.finished`, `payment.completed`, `shift.started`, `shift.ended`, `ride.accepted` (and their Go structs) don't exist yet.

Check `init.sql`/`contracts` directly before writing code against any field/topic mentioned in the README that isn't confirmed there.

List endpoints (`GET /users` on auth-service, `GET /ride`, `GET /driver-profile`, `GET /driver-shift`) now return `contracts.PagedResponse[T]` (`{items, page, pageSize, totalCount}`), not a bare array — bare-array list responses documented elsewhere in this repo's history are stale. driver-service's `GetList`/`GetByID` handlers now serialize the camelCase `contracts.DriverProfileDto`/`ShiftDto`, not raw `domain.*` structs — the PascalCase wire-format quirk mentioned in older notes no longer applies.

Several real bugs were found and fixed while verifying docs against code (2026-07-14) — mentioned here so they aren't "rediscovered" as still-open issues: `ShiftUpdatedEvent.DriverID`'s json tag was `clientId` (now `driverId`); `driver-service`'s `ShiftHandler.GetList`/`GetByID` were calling the driver-profile queries instead of the shift queries (copy-paste bug); `ShiftHandler.Update` was using the request body's `DriverId` field as the shift ID instead of the path `{id}`; the outbox worker (`internal/workers/shift_updated_outbox_worker.go`) was a non-compiling Stage-1 stub never wired into `main()` — it's now a real Stage-2 `OutboxWorker` (see "Transactional outbox pattern" above and `PLAN.md`); and the producer/consumer topic name mismatch (`driver.shifts.updated` vs. `shift.updated`) is resolved in favor of `shift.updated`. A second pass (2026-07-18, prompted by building the e2e-test simulator) fixed auth-service's login/refresh, which had never worked: unqualified `"user"`/`refresh_token` table names (no search_path in the DSN), the UUID user id scanned into an `int` and stuffed into the JWT as a number, and `/refresh` returning a bare string instead of `RefreshResponse` — see PLAN.md's 2026-07-18 section.

### Matching algorithm status

The README documents a full target matching algorithm (radius discovery → weighted ranking → tiered broadcast → atomic Redis accept → expanding-radius retry → per-driver rate limiting). None of that exists yet. Current `matching-service` (verified in detail):
- Consumes `ride.requested` (via `RideAcceptedConsumer` — misnamed, it doesn't consume `ride.accepted`) and `shift.updated` (via `ShiftUpdatedConsumer`); both hardcode their topic string internally and ignore the `topic` argument passed to `Run(...)`.
- On `ride.requested`, `CreateRideHandler` just caches the full event into Redis as a hash at `ride:{rideId}` (fields include a hardcoded `status:"searching", radius:3000, attempt:1`) — no ranking, offering, or acceptance logic at all.
- On `shift.updated`, `CreateDriverHandler` caches `{shiftID, status, updatedAt}` into Redis at `driver:{driverID}`.
- No query handlers, no Kafka producer (nothing is ever published), no `POST /rides/{rideId}/accept` HTTP endpoint, no HTTP layer at all in this service yet.
- Postgres is opened in `cmd/main.go` but never used by the live code path — only by the dead `handleRideRequested` function.

Building that algorithm and cleaning up the dead code is the immediate next step — see `PLAN.md`.

### The layered pattern (driver-service, and the direction matching-service is moving)

```
internal/
  domain/            entities + repository interfaces (ports), e.g. DriverProfileRepository, ShiftRepository, OutboxRepository
  application/
    command/         one file per write use case; constructor wraps the handler with decorators
    query/            one file per read use case
    services/         cross-cutting app-layer ports, e.g. TransactionManager interface
    app.go             wires Commands/Queries structs of decorator.CommandHandler[Cmd, Result] / QueryHandler[...]
  common/
    decorator/        generic CommandHandler[C,R]/QueryHandler[Q,R] interfaces + logging & metrics decorators applied at construction time
    errors/
  persistence/        Postgres repository implementations + PostgresTransactionManager (context-based tx propagation via WithTx/context_helpers.go)
  infrastructure/      health checks, graceful shutdown, metrics client
  interfaces/http/handler/  thin HTTP handlers that decode a request, build a command/query, call application.Commands/Queries, encode the response
  workers/            outbox-polling background workers (e.g. `OutboxWorker`: ticker + `select` loop, drains `outbox_message` inside a transaction, publishes via an `EventPublisher` port)
cmd/main.go           composition root: opens DB, builds repositories, wraps them into `app.Application`, registers HTTP routes, starts server + workers
```

Command/query handlers are constructed via `NewXHandler(...)` functions that build the concrete handler and immediately wrap it with `decorator.ApplyCommandDecorators`/`ApplyCommandDecoratorsNoResult` (logging + metrics) — callers only ever see the decorated `decorator.CommandHandler[C,R]` interface, never the concrete type. Follow this shape for any new command/query in driver-service or matching-service.

`services.TransactionManager.WithinTransaction(ctx, fn)` opens a Postgres tx, stashes it on the context (`persistence.WithTx`), and runs `fn`; repository methods pull the tx off the context via `context_helpers.go` so the same repository code works inside or outside a transaction.

### Auth

JWT-based (`golang-jwt/jwt/v5`), HS256, secret from `JWT_SECRET` env var (default `secret_change_me` — never rely on this outside local dev). Access + refresh tokens are both plain JWTs; refresh tokens are additionally persisted in `auth.refresh_token` so they can be revoked/validated server-side. Downstream services expect the caller (gateway, in the target design) to forward an `X-User-Id` header — `ride-service`'s `POST /request-ride` reads it directly rather than validating the JWT itself.
