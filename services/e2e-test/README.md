# e2e-test — continuous client-activity simulator

A load simulator that behaves like real MyUberGo users: N virtual **clients** sign up, log in, and request rides; M virtual **drivers** sign up, create profiles, and cycle shifts (open → Online → Ended). Every write is **deep-verified** — read back via the service's GET endpoints and asserted field by field — and results are aggregated into a periodic stats report.

It is deliberately **not** part of docker-compose: a load generator that autostarts with the stack is unwanted during development. Run it manually against a running stack.

## Run

```bash
# from repo root
docker-compose up --build          # start the stack first

cd services/e2e-test
go run ./cmd                       # defaults: 5 clients, 3 drivers
go run ./cmd -clients 10 -drivers 5 -ride-interval 2s
```

Stop with Ctrl-C — actors drain gracefully and a final report is printed.

## Configuration

Flags override environment variables; both are optional.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-auth-url` | `E2E_AUTH_URL` | `http://localhost:8000` | auth-service base URL |
| `-ride-url` | `E2E_RIDE_URL` | `http://localhost:8001` | ride-service base URL |
| `-driver-url` | `E2E_DRIVER_URL` | `http://localhost:8003` | driver-service base URL |
| `-matching-url` | `E2E_MATCHING_URL` | `http://localhost:8002` | matching-service base URL |
| `-clients` | `E2E_CLIENTS` | `5` | number of virtual clients |
| `-drivers` | `E2E_DRIVERS` | `3` | number of virtual drivers |
| `-ride-interval` | `E2E_RIDE_INTERVAL` | `5s` | base interval between rides per client (jittered ±50%) |
| `-shift-interval` | `E2E_SHIFT_INTERVAL` | `15s` | base interval between shift cycles per driver (jittered ±50%) |
| `-report-interval` | `E2E_REPORT_INTERVAL` | `10s` | stats report interval |
| `-run-for` | `E2E_RUN_FOR` | `0` (forever) | stop automatically after this duration — for timed/CI runs |
| `-seed` | — | wall clock | base random seed (per-actor seeds derive from it) |

## What each actor does

**Client** (`client-N`): signup (`Client` role, unique per-run email) → login → loop: `POST /request-ride` with randomized coordinates → `GET /ride/{id}` asserting the full field round-trip, status `Requested`, no driver assigned, price > 0. Every 5th iteration checks the ride appears in `GET /ride`; every 10th refreshes the access token.

**Driver** (`driver-N`): signup (`Driver` role) → login → `POST /driver-profile` → GET-verify → loop: `POST /driver-shift/create` → GET-verify (`endedAt` empty) → `PUT /driver-shift/{id}` status `Online` (emits `shift.updated` through the outbox → Kafka) → work period, during which it polls `GET /drivers/{driverId}/offer` on matching-service every ~2s (op `matching.offer.get`) → status `Ended` → GET-verify `endedAt` set. Every 4th cycle updates the profile phone and verifies the round-trip.

When a poll during the work period finds an offer, the driver accepts it via `POST /rides/{rideId}/accept` (op `matching.ride.accept`) and deep-verifies: the offer disappears (`GET /drivers/{driverId}/offer` now 404s, another `matching.offer.get`) and a duplicate `POST /rides/{rideId}/accept` now 409s (op `matching.ride.accept.dup`). A 409 on the *first* accept just means another driver won the race for that ride; it's recorded under `matching.ride.accept` as a legitimate outcome, not a failure.

HTTP failures and verification mismatches are logged per actor and counted per operation in the report — they never kill the process, so the simulator can be started before/while the stack is coming up.

## Encoded service quirks

- `driver.shift` has no `status` column: only `"Ended"` persists anything (`ended_at`); other statuses just emit `shift.updated`. Shift end is verified via `endedAt`, never via a status round-trip.
- ride-service does not validate JWTs — it trusts the `X-User-Id` header, so the ride client sends the raw user ID (tokens are still exercised via login/refresh).
- Emails embed a run ID (`client-{runID}-{i}@e2e.local`) because `auth.user.email` is unique and the DB persists across runs.
- ride-service doesn't consume `ride.accepted` yet, so once a driver accepts a ride the simulator has no way to verify that ride's Postgres status actually reaches `Matched` — that check lands once ride-service adopts Stage 2.

## Observing side effects

- Kafka UI (http://localhost:8080): `ride.requested` and `shift.updated` message counts grow while the simulator runs.
- `GET http://localhost:8001/ride` shows the simulated rides.
