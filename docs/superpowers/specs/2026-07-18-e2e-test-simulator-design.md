# e2e-test: continuous client-activity simulator

## Context

MyUberGo has no test suite and no automated way to exercise the auth → driver → ride → Kafka pipeline; the Bruno collection is the only manual client. This design adds `services/e2e-test`: a Go module acting as a **continuous load simulator** — N virtual clients and M virtual drivers, each a goroutine running a realistic lifecycle loop against the running docker-compose stack, with **deep verification** (every write is read back via GET and asserted) and periodic stats reporting. It is also the repo's first real exercise of Stage-3 concurrency patterns: goroutine-per-actor, channel-fed stats aggregator, context cancellation, WaitGroup shutdown.

## Decisions (user-confirmed during brainstorming)

- **Continuous simulator**, not a one-shot pass/fail test.
- **Deep verification** after each write, with mismatches logged and counted.
- **Goroutine-per-actor**, actor counts configurable via env/flags.
- **Local `go run` only** — deliberately NOT in docker-compose; a load generator autostarting with the stack is unwanted during development.
- **Structure**: `cmd/main.go` composition root + small `internal/` packages (`apiclient`, `actors`, `stats`). Not Stage-1 single-file (no server to anchor it), not Stage-2 CQRS (no domain, no persistence).
- Reuses `github.com/oxf/MyUber/contracts/http` DTOs directly via the standard `replace => ../contracts` pattern — exercising the contracts module the way a real external consumer would.

## API surface driven

- auth :8000 — `POST /signup`, `POST /login`, `POST /refresh` (roles `Client`/`Driver`).
- driver :8003 — `POST /driver-profile`, `PUT /driver-profile/{id}`, `GET /driver-profile[/{id}]`, `POST /driver-shift/create`, `PUT /driver-shift/{id}`, `GET /driver-shift[/{id}]`.
- ride :8001 — `POST /request-ride` (needs `X-User-Id` header; JWTs are not validated by ride-service), `GET /ride?page=&pageSize=`, `GET /ride/{id}`.

## Encoded quirks (verified in code at design time)

1. `driver.shift` has no `status` column — `PUT /driver-shift/{id}` with `"Ended"` sets `ended_at`; other statuses persist nothing but emit a `shift.updated` outbox event. Shift end is verified via `endedAt`, never via status round-trip.
2. Ride status literal is `"Requested"`; no driver is ever assigned today (matching-service only caches events), so the client actor verifies the ride exists correctly and moves on — no polling for assignment.
3. Emails must be unique per run (`{role}-{runID}-{i}@e2e.local`) because `auth.user.email` is unique and the DB persists across runs.
4. `CreateShift` rejects a second active shift per driver, so the driver actor always ends its shift before opening the next.

## Actor lifecycles

**ClientActor** (default 5): signup → login → loop { request ride with randomized coords → verify response (`clientId`, `status "Requested"`) → `GET /ride/{id}` and assert full field round-trip, `estimatedPrice > 0`, `driverId` nil; every ~10th iteration refresh the token; every ~5th list rides and assert the latest own ride appears } with jittered sleep.

**DriverActor** (default 3): signup → login → create profile → GET and assert round-trip (status `Offline`) → loop { create shift → GET assert `driverId` matches and `endedAt` empty → PUT status `Online` (200 only; feeds `shift.updated` → Kafka) → work period → PUT status `Ended` → GET assert `endedAt` set; occasionally update profile phone and assert round-trip } with jittered sleep.

Every step emits a `stats.Event`; actor errors are logged and counted, never fatal.

## Runtime shape

Config via env with flag overrides: `E2E_AUTH_URL`/`E2E_RIDE_URL`/`E2E_DRIVER_URL` (default `http://localhost:8000|8001|8003`), `E2E_CLIENTS`, `E2E_DRIVERS`, `E2E_RIDE_INTERVAL`, `E2E_SHIFT_INTERVAL`, `E2E_REPORT_INTERVAL`. `signal.NotifyContext` for SIGINT/SIGTERM → actors drain via context cancellation → `wg.Wait()` → close stats channel → final report → exit 0. Per-actor `*rand.Rand` seeded from base seed + index.

## Out of scope

- No docker-compose/Dockerfile for this module.
- No waiting for driver assignment until matching-service actually assigns drivers.
- No Bruno collection changes (no service endpoints change).
- No `*_test.go` files — this is a running simulator, not a `go test` suite.
