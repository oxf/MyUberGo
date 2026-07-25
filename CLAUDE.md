# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

MyUberGo is a learning project: a distributed, event-driven clone of Uber built as independent Go microservices communicating over Kafka (async) and REST/HTTP (sync), backed by PostgreSQL (per-service schemas in one instance) and Redis. It is deliberately mid-refactor — services are in different architectural stages on purpose (see "Architectural maturity" below), so don't assume every service should look like the others.

The target system has **8 services**; only **4 are implemented** (auth, ride, matching, driver — this repo's `services/` directory). Location, Billing, and Notification are planned but not scaffolded; an API Gateway (Kong) now sits in front of the 4 implemented services — see "API Gateway (Kong)" below. The full target design — per-service responsibilities, schemas, HTTP/Kafka contracts, the matching algorithm, and the phased roadmap — lives in `README.md`; don't duplicate it here. This file only calls out where the **current code** (what you'll actually be editing) diverges from that target, so you don't assume an unbuilt table/endpoint/topic already exists.

**See `PLAN.md` for the step-by-step, checkable implementation roadmap** (what to build next, in what order). This file (CLAUDE.md) is the architectural orientation; PLAN.md is the working task list — keep them consistent but don't duplicate PLAN.md's step list here.

## Commands

There is no root build tool, Makefile, or CI config — each service is built/run independently.

```bash
# Full stack (Postgres, Redis, Kafka, Kafka UI, all 4 services, an API Gateway, and the admin dashboard)
docker-compose up --build

# API Gateway (Kong) — the only ingress for auth/ride/driver: http://localhost:8090/api/{auth,ride,driver}/...
# Admin dashboard: http://localhost:5173
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

`web/` at the repo root is a Vite + React + TypeScript read-only admin dashboard (Users / Drivers / Shifts / Rides tables with server-side paging + sorting via the shared `PagedResponse[T]` envelope in `contracts/http`). It's in `docker-compose.yml` as the `web` service — a multi-stage build (`web/Dockerfile`) that runs `npm run build` and serves the static bundle from nginx on port 5173 (mapped to container port 80), matching the other services' build-a-lean-image-from-repo-context pattern rather than shipping the Vite dev server. For active frontend development (hot reload) run it manually instead, against a stack already up via `docker-compose up`:

```bash
cd web
npm install
npm run dev   # http://localhost:5173 — stop/exclude the `web` compose service first, or use a different port, to avoid a 5173 clash
```

Same-origin API proxying is implemented twice, deliberately kept in sync: the Vite dev server proxies all `/api/*` → Kong on `localhost:8090` (`web/vite.config.ts`), since `npm run dev` runs on the host; the containerized build does the same proxying in `web/nginx.conf`, pointing at the compose service name `kong:8000` instead of `localhost`, since nginx runs inside the compose network. Kong does its own path-prefix stripping and JWT verification either way, so neither proxy config needs per-service routing rules — the Go services need no CORS headers either, and that stays true regardless of Kong's route table. TypeScript DTOs in `web/src/api/types.ts` mirror `services/contracts/http` json tags — update them whenever contracts change. List-endpoint paging contract (all services): 1-based `page`, `pageSize` (default 20, cap 100), `sortBy` validated against a per-endpoint whitelist, `sortDir` asc|desc. The four list endpoints (`GET /users`, `/ride`, `/driver/driver`, `/driver/driver-shift`) are Admin-only at the Kong gateway (see "API Gateway (Kong)" below) — the dashboard only works logged in as the seeded admin account.

## e2e-test simulator

`services/e2e-test` is a continuous client-activity simulator, not a business service: goroutine-per-actor virtual clients and drivers drive auth/driver/ride over HTTP (reusing the `contracts/http` DTOs), deep-verify every write by reading it back, and report per-op stats. It is deliberately **not** in docker-compose — run it manually (`go run ./cmd` from `services/e2e-test`) against the running stack; see its README for config. It sits outside the 3-stage service progression (no server, no DB, no domain — just `cmd` + `internal/apiclient|actors|stats`). When you add or change a service endpoint, extend the corresponding actor/apiclient so the simulator keeps covering it.

## Architecture

### Services and ports

| Service | Port | Role |
|---|---|---|
| auth-service | 8000 | User signup/login, JWT issuance (access + refresh), refresh-token rotation, `GET /me`, `POST /logout` |
| ride-service | 8001 | Ride requests, fare/distance estimation, ride lifecycle, publishes `ride.requested` |
| matching-service | 8002 | Consumes `ride.requested`/`shift.updated`, matches rides to available drivers, publishes `ride.accepted`; `POST /rides/{rideId}/accept` + `GET /drivers/{driverId}/offer` |
| driver-service | 8003 | Driver profiles and shifts (login/logout, rides, earnings), publishes `shift.updated` |

`services/contracts` is a shared Go module (no `replace` of its own) holding the wire contracts every service depends on:
- `contracts/http` — REST request/response DTOs per service (`auth-service.go`, `driver-service.go`, `ride-service.go`, `matching-service.go` — `AcceptRideRequest`/`AcceptRideResponse`/`DriverOfferDto`)
- `contracts/kafka` — Kafka event payloads (`ride-service.go` has `RideRequestedEvent`; `driver-service.go` has `ShiftUpdatedEvent`, now with a `Rating` field; `matching-service.go` has `RideAcceptedEvent`)

Changing a field used across services means editing it once in `contracts` — both the producer and consumer(s) pick it up through the `replace` directive.

### API Gateway (Kong)

Kong (`gateway/kong.yml`, declarative config, `_format_version: "3.0"`) fronts auth-service, ride-service, and driver-service as the sole host-reachable ingress, on `:8090` (mapped from Kong's internal `:8000` in `docker-compose.yml`) — those 3 services no longer publish host ports at all; the only way to reach them from outside the Docker network is through Kong. matching-service is the one exception: it's still reached directly on `:8002` (Kafka/Redis-driven internally, no gateway-facing route — see "Matching algorithm status" below).

- **Public routes** (no token, IP-rate-limited): `POST /api/auth/signup`, `/login`, `/refresh` — a caller has no token yet at this point in the flow.
- **Protected routes** (`jwt` plugin + header-injecting `post-function` + per-user rate-limiting): `GET /api/auth/me`, `POST /api/auth/logout`, and everything under `/api/ride`, `/api/driver`. Kong's `jwt` plugin verifies the bearer token's signature/`exp`, matched by the token's `iss` claim against the one configured consumer (`myubergo-app`) in `gateway/kong.yml`. A shared `post-function` Lua snippet (the `&inject_user_headers` YAML anchor) then decodes the token's payload and sets `X-User-Id`/`X-User-Email`/`X-User-Role`/`X-Client-Id` on the proxied request, overwriting any of those headers the caller sent directly. `X-Client-Id` is only set for Client-role tokens (whose `client_id` claim is non-empty — see "Auth" below); ride-service's `POST /request-ride`/`DELETE /ride/{id}` read it instead of `X-User-Id`, since `ride.ride.client_id` is `auth.client(id)`, not `auth.user(id)`.
- **Admin-only routes** (`jwt` + `inject_user_headers` + a second `&require_admin` Lua snippet + per-user rate-limiting): `GET /api/auth/users`, `GET /api/ride`, `GET /api/driver/driver`, `GET /api/driver/driver-shift`. `require_admin` 403s unless the injected `X-User-Role` is `Admin`. These four routes match on **path and method** (`methods: [GET]`, anchored regex paths like `~/api/ride$`), which Kong's traditional router ranks above the broader paths-only protected routes below — so e.g. `GET /api/ride` hits the admin-gated route while `POST /api/ride/request-ride` and `GET /api/ride/{id}` still hit the ordinary one.
- Longer/more-specific routes win regardless of declaration order in the file, so `/api/auth/me`/`/api/auth/logout` match ahead of the shorter public `/api/auth` route, and the four admin routes above match ahead of the broader `/api/ride`/`/api/driver` routes, even though both are declared after in `kong.yml`.
- Downstream services never validate the JWT themselves — they trust the `X-User-Id`/`X-User-Email`/`X-User-Role`/`X-Client-Id` headers Kong injects, and never check `X-User-Role` for authorization either (that's enforced entirely at the gateway via `require_admin`). This is only safe because Kong is the sole ingress: don't publish a host port for auth/ride/driver-service without re-adding real auth to that service, or the trust boundary breaks.
- `JWT_SECRET` (auth-service's env var) and `gateway/kong.yml`'s `jwt_secrets.secret` are two independent config values that must be kept in sync manually (both default to `secret_change_me` locally) — and auth-service's minted `iss` claim (`"myubergo-auth"`, `auth-service/internal/infrastructure/security/jwt_issuer.go`) must keep matching the `jwt_secrets` key name in `kong.yml`, or every token fails the `jwt` plugin.

### Data model

A single Postgres instance hosts one schema per service (`auth`, `ride`, `driver`; `matching` is referenced by matching-service SQL but not yet defined in the shared migration — check `services/shared/migrations/init.sql` before assuming a table exists). That file is the single source of truth for schema and is mounted into the Postgres container's `docker-entrypoint-initdb.d` — there is no migration tool/versioning, just this one SQL file run on first container start.

`auth.user` is the shared login identity for every role (`role CHECK IN ('Client','Driver','Admin')`) — it holds only account-level fields (email, password hash, name, phone). Each role's own data lives in a separate, surrogate-keyed table: `auth.client` and `auth.admin` (both `id UUID PK`, `user_id UUID UNIQUE FK -> auth.user`), and `driver.driver` (same shape, in the `driver` schema since driver-service owns it). `ride.ride.client_id` references `auth.client(id)`, and `ride.ride.driver_id` references `driver.driver(id)` — both are role-table ids, never `auth.user(id)`. A signup with `role: Client` creates `auth.user` + `auth.client` atomically (auth-service's `SignupHandler`, wrapped in a `TransactionManager` — see "Architectural maturity" below); a `Driver` signup only creates `auth.user`, and the `driver.driver` row comes later via `POST /api/driver/driver`, since that table lives in a schema auth-service doesn't own. There is no signup path for `Admin` — `NewUser` rejects it — the one seeded admin account (`admin@myubergo.local` / `admin123` locally) is inserted directly by `init.sql`.

### Transactional outbox pattern

`ride-service` and `driver-service` write domain state and an outbox row in the same DB transaction, then a background worker (`internal/workers`, ticker + `select` loop) polls `outbox_message` (`FOR UPDATE SKIP LOCKED` inside `TransactionManager.WithinTransaction`), publishes to Kafka via an `EventPublisher` port/adapter, and marks rows processed (or increments `retries` on publish failure — no dead-letter handling yet). This is how `ride.requested` and `shift.updated` events get published reliably. `matching-service` consumes both topics on the other end (caching driver/ride state in Redis, `internal/infrastructure/cache`) and publishes its own `ride.accepted` event, but without an outbox: it publishes directly from the `AcceptRideHandler` at the point a ride is matched, since Redis has no transaction to hide a dual write behind the way Postgres does here — see "Matching algorithm status" below for that tradeoff.

### Architectural maturity: the 3-stage learning progression (intentional, not inconsistency)

Every service is deliberately built through 3 stages, and different services currently sit at different stages on purpose — match a service's *current* stage when extending it rather than unilaterally jumping it ahead:

- **Stage 1 — Basic/procedural**: everything in a single `cmd/main.go`. Handlers decode a request, run raw SQL directly, encode a response. No domain/application separation.
- **Stage 2 — CQRS + DDD (Clean Architecture)**: layered into `domain`/`application`(`command`+`query`)/`persistence`/`interfaces`/`infrastructure` (see "The layered pattern" below), command/query handlers wrapped with logging+metrics decorators, a `TransactionManager` for context-propagated DB transactions, graceful shutdown, and health checks.
- **Stage 3 — Production-grade advanced Go** (not started anywhere yet): idiomatic concurrency (goroutines/channels/`select` — e.g. fan-out/fan-in for broadcasting ride offers, worker pools for outbox pollers), code generation from OpenAPI (HTTP contracts) and/or gRPC (inter-service calls) instead of hand-written structs, a real metrics/tracing backend (current metrics client is a logging-only stub, not Prometheus), and a real liveness signal (current health checker hardcodes `Live: true` and only ever reflects readiness via DB ping).

| Service | Current stage | Notes |
|---|---|---|
| auth-service | Stage 2 | full CQRS/DDD layering mirroring ride-service, deliberately **without** an outbox (auth publishes no events); JWT/bcrypt logic moved behind `application/services` ports (`PasswordHasher`, `TokenIssuer`) with adapters in `infrastructure/security`; adds `GET /me`, `POST /logout`, `auth.user.updated_at`/`deleted_at` soft-delete columns, and (as of the 2026-07-25 role-table refactor) a `TransactionManager` so `Signup` can write `auth.user` + `auth.client` atomically |
| ride-service | Stage 2 | full CQRS/DDD layering (`domain`/`application`/`persistence`/`interfaces`/`workers`), a transactional-outbox worker for `ride.requested`, and `ride.accepted`/`ride.cancelled` consumers; ride lifecycle now spans `Requested → Matched → InProgress → Completed`/`Cancelled` |
| driver-service | Stage 2 (+ early Stage-3 features) | the reference Stage-2 implementation; already has graceful shutdown, health checks, logging/metrics decorators, and a working transactional-outbox worker grafted on, but metrics is a logging stub and health's liveness check is a no-op (see infra notes below) |
| matching-service | Stage 2, complete for its current scope | full CQRS layering: command/query handlers wrapped with decorators, Redis-backed repos, an HTTP layer (`POST /rides/{rideId}/accept`, `GET /drivers/{driverId}/offer`), a Kafka producer (`ride.accepted`), graceful shutdown, and a Redis-based health checker. Dead Stage-1 code and the Postgres dependency are gone. Implements a simplified version of the README's matching algorithm — see "Matching algorithm status" below for exactly what's simplified. |

As of 2026-07-25 all 4 implemented services are Stage 2 — Stage 3 (see above) is the open frontier for all of them now, not just driver-service. See `PLAN.md` for the ordered, checkable steps to move each service forward. Location, Billing, and Notification services described in the README's target design don't exist in `services/` yet at all (Phase 3 of the README roadmap) — don't assume their directories, schemas, or Kafka topics are present.

### Target vs. current schema/contracts

The README's per-service target design is ahead of what's actually in `services/shared/migrations/init.sql` and `services/contracts`. Notably not yet present in the current schema/contracts even for the 4 implemented services:
- `driver.driver` (renamed from `driver_profile` in the 2026-07-25 role-table refactor — no longer stores `name`/`phone` either, since `auth.user` is now the single source for those): no `license_number`/`license_expiry`/`vehicle_color`; no `driver.driver_rating` table.
- `ride.ride`: no separate `RIDE_REQUEST` table, no time-windowed tariffs (only one flat rate is used, `ride.tariff` isn't read by the handler).
- **No `matching` schema anywhere, full stop** — not just "not yet defined": `init.sql` only declares `auth`/`ride`/`driver`; matching-service has never had a Postgres dependency in its live code, and the module no longer imports `lib/pq` at all. Don't add code that assumes a `matching` schema exists.
- `services/contracts/kafka` defines `RideRequestedEvent`, `ShiftUpdatedEvent` (now with a `Rating float64` field, populated by driver-service and consumed by matching-service's rating-only ranking), and `RideAcceptedEvent` (matching-service's first Kafka producer output — `{rideId, driverId, acceptedAt}`). `ride.cancelled`, `ride.started`, `ride.finished`, `payment.completed`, `shift.started`, `shift.ended` still don't exist yet. `ride.accepted` is published but has no consumer anywhere yet (`ride-service` would need it to flip a ride to `Matched`, but is still Stage 1).

Check `init.sql`/`contracts` directly before writing code against any field/topic mentioned in the README that isn't confirmed there.

List endpoints (`GET /users` on auth-service, `GET /ride`, `GET /driver/driver`, `GET /driver/driver-shift`) now return `contracts.PagedResponse[T]` (`{items, page, pageSize, totalCount}`), not a bare array — bare-array list responses documented elsewhere in this repo's history are stale, and (as of the 2026-07-25 role-table refactor) all four are Admin-only at the Kong gateway. driver-service's `GetList`/`GetByID` handlers now serialize the camelCase `contracts.DriverDto`/`ShiftDto`, not raw `domain.*` structs — the PascalCase wire-format quirk mentioned in older notes no longer applies.

Several real bugs were found and fixed while verifying docs against code (2026-07-14) — mentioned here so they aren't "rediscovered" as still-open issues: `ShiftUpdatedEvent.DriverID`'s json tag was `clientId` (now `driverId`); `driver-service`'s `ShiftHandler.GetList`/`GetByID` were calling the driver-profile queries instead of the shift queries (copy-paste bug); `ShiftHandler.Update` was using the request body's `DriverId` field as the shift ID instead of the path `{id}`; the outbox worker (`internal/workers/shift_updated_outbox_worker.go`) was a non-compiling Stage-1 stub never wired into `main()` — it's now a real Stage-2 `OutboxWorker` (see "Transactional outbox pattern" above and `PLAN.md`); and the producer/consumer topic name mismatch (`driver.shifts.updated` vs. `shift.updated`) is resolved in favor of `shift.updated`. A second pass (2026-07-18, prompted by building the e2e-test simulator) fixed auth-service's login/refresh, which had never worked: unqualified `"user"`/`refresh_token` table names (no search_path in the DSN), the UUID user id scanned into an `int` and stuffed into the JWT as a number, and `/refresh` returning a bare string instead of `RefreshResponse` — see PLAN.md's 2026-07-18 section. A third pass (2026-07-19, building the matching algorithm) fixed driver-service's `UpdateShiftHandler`: setting a shift to `Ended` short-circuited before the outbox insert, so ending a shift never published a `shift.updated` event at all — matching-service's online-driver pool would only ever grow, never shrink, since it never learned a driver went offline. Both branches now share the same transaction and outbox-insert flow. A fourth pass (2026-07-25, refactoring auth-service to Stage 2 and adding `GET /me`/`POST /logout`) found `services/auth-service/Dockerfile`'s builder stage still only did `COPY services/auth-service/cmd ./cmd` — a leftover from the Stage-1 single-file build — so the container image build broke immediately once `internal/` packages existed (`package auth-service/internal/... is not in std`); fixed to `COPY services/auth-service .`, matching every other Stage-2 service's Dockerfile. This same pass also corrected this file's stale claims that ride-service was still Stage 1 (it's been fully Stage-2 layered since 2026-07-21 per `PLAN.md`) and that the API Gateway was merely planned (Kong has been live since the prior session, just undocumented here until now). A fifth pass (2026-07-25, the role-table refactor) split `auth.user` from role-specific data: added `auth.client`/`auth.admin` (surrogate-keyed, `user_id UNIQUE FK`), renamed `driver.driver_profile` to `driver.driver` and dropped its `name`/`phone` columns, repointed `ride.ride.client_id` at `auth.client(id)` (was `auth.user(id)`), and added the `Admin` role gated to Kong-only list routes. This was the first time auth-service needed a `TransactionManager` (signup now writes two tables atomically for a Client) and the first time ride-service read an `X-Client-Id` header instead of `X-User-Id`.

### Matching algorithm status

The README documents a full target matching algorithm (radius discovery → weighted ranking → tiered broadcast → atomic Redis accept → expanding-radius retry → per-driver rate limiting). `matching-service` now implements a simplified version of that same shape, entirely against Redis (no Postgres anywhere in this service — removed from `go.mod`, `cmd/main.go`, and `docker-compose.yml`):

- **Consumers**: `RideRequestedConsumer` (renamed from the misleading `RideAcceptedConsumer`) consumes `ride.requested`; `ShiftUpdatedConsumer` consumes `shift.updated`. Both honor the `topic` argument passed to `Run(ctx, topic)` (no longer hardcoded) and use a cancellable context for graceful shutdown.
- **Discovery is rating-only, not geo**: no Location service exists, so there's no real distance data. `ShiftUpdatedEvent` now carries the driver's `rating`; `UpsertDriver` (renamed from `CreateDriver`) maintains a `drivers:online` Redis ZSET (driverID → rating), adding a driver on `status:"Online"` and removing them on any other status.
- **On `ride.requested`**: `CreateRideHandler` caches the ride into Redis (`ride:{rideId}` hash, `status:"searching"`), then `BroadcastOffersHandler` immediately runs the first offer round: pulls the top `attempt×5` rating-ranked online drivers, filters out already-offered/busy/rate-limited candidates, offers to the top 5 (`ride:{rideId}:offered_drivers` SET, `driver:{driverId}:current_offer` STRING with a 30s TTL), and arms a retry deadline (`pending_ride:{rideId}` hash).
- **Broadcasting is BROADCAST-only** (top 5 at once, first accept wins) — the README's TIERED strategy and Stage-3 concurrent goroutine/channel fan-out for the broadcast step are not implemented yet.
- **Atomic accept**: `POST /rides/{rideId}/accept` → `AcceptRideHandler` does a Redis `SET ride:{rideId}:accepted_by driverId NX` (first writer wins); 409 on a lost race, 400 on an expired/cancelled/not-offered-to-this-driver claim, 404 if the ride doesn't exist. On success it marks the ride `matched`, clears the offer/pending state, removes the driver from `drivers:online`, and publishes `ride.accepted` directly (no outbox — a deliberate at-most-once tradeoff, since Redis has no transaction to hide a dual write behind and the match itself is already durable).
- **Retry** is a background `MatchRetryWorker` (ticker+select loop) sweeping `pending_ride:*`; a ride whose deadline lapsed without an accept gets `BroadcastOffers` re-run with `attempt+1` (widening the candidate pool instead of expanding a geo radius), capped at 5 attempts before the ride is marked `failed` and the retry gives up.
- **Rate limiting**: `driver:{driverId}:notifications:minute` INCR/EXPIRE, capped at 3/minute — implemented as a sliding window (each offer resets the 60s TTL) rather than a strict fixed window.
- **`ride:{rideId}:cancelled`** is checked on accept but nothing sets it yet — no `ride.cancelled` consumer exists anywhere in the codebase.
- **`GET /drivers/{driverId}/offer`** lets a driver poll for their current offer (404 if none) — there's no Notification service to push offers, so polling is the only delivery mechanism for now.
- **Now wired end-to-end on the ride side**: `ride-service` consumes `ride.accepted` (`internal/consumers/ride_accepted_consumer.go`) and flips the matched ride's Postgres row to `driver_id`/`status='Matched'`/`matched_at` via a new `MarkRideMatched` command, guarded to be idempotent against redelivery. `driver-service` also consumes it (`internal/consumers/ride_accepted_consumer.go` → `ProcessRideAccepted` command), but that handler is currently a logging-only placeholder — there's no persisted "driver is on a ride" state today (`driver.driver.status` only allows `Offline`/`Online`) and no `ride.completed`/`ride.cancelled` event yet to ever reverse it.

Remaining work toward the README's full target: geo-based discovery (needs the Location service), TIERED broadcast escalation, and the Stage-3 concurrent fan-out conversion (goroutines/channels/`select` for broadcasting offers) — see `PLAN.md`.

### The layered pattern (driver-service, and the direction matching-service is moving)

```
internal/
  domain/            entities + repository interfaces (ports), e.g. DriverRepository, ShiftRepository, OutboxRepository
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

JWT-based (`golang-jwt/jwt/v5`), HS256, secret from `JWT_SECRET` env var (default `secret_change_me` — never rely on this outside local dev). Access + refresh tokens are both plain JWTs; refresh tokens are additionally persisted in `auth.refresh_token` so they can be revoked/validated server-side — `POST /logout` deletes one, scoped to the caller's own `X-User-Id` (never a token-body field, so a caller can only revoke their own sessions). `GET /me` returns the caller's own profile, likewise identified by `X-User-Id`, and additionally echoes `clientId` (looked up from `auth.client` by `Login`/`Refresh`/`GET /me` — never for `Driver`/`Admin` accounts, which have no client row). Every JWT's `iss` claim is `"myubergo-auth"` (`auth-service/internal/infrastructure/security/jwt_issuer.go`) and must stay in sync with the `jwt_secrets` key configured for Kong's consumer in `gateway/kong.yml`, or every token fails Kong's `jwt` plugin. A Client's access token additionally carries a `client_id` claim (empty for `Driver`/`Admin`); Kong injects it as `X-Client-Id`. Downstream services never validate the JWT themselves — Kong does that at the edge and injects `X-User-Id`/`X-User-Email`/`X-User-Role`/`X-Client-Id` from the token's own claims (see "API Gateway (Kong)" above); `ride-service`'s `POST /request-ride` reads `X-Client-Id` (not `X-User-Id`, since `ride.ride.client_id` is now `auth.client(id)`), and every other protected handler reads whichever injected header matches the id it needs.
