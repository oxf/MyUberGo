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
| `-auth-url` | `E2E_AUTH_URL` | `http://localhost:8090/api/auth` | auth-service base URL, via the Kong gateway |
| `-ride-url` | `E2E_RIDE_URL` | `http://localhost:8090/api/ride` | ride-service base URL, via the Kong gateway |
| `-driver-url` | `E2E_DRIVER_URL` | `http://localhost:8090/api/driver` | driver-service base URL, via the Kong gateway |
| `-matching-url` | `E2E_MATCHING_URL` | `http://localhost:8002` | matching-service base URL — reached directly, it has no gateway route (see `gateway/kong.yml`) |
| `-billing-url` | `E2E_BILLING_URL` | `http://localhost:8090/api/billing` | billing-service base URL, via the Kong gateway |
| `-location-url` | `E2E_LOCATION_URL` | `http://localhost:8090/api/location` | location-service base URL, via the Kong gateway |

auth/ride/driver/billing/location now go through Kong (see the repo-root `gateway/` directory and CLAUDE.md's "API Gateway" section) rather than being hit directly — their host ports are no longer published by docker-compose.
| `-clients` | `E2E_CLIENTS` | `5` | number of virtual clients |
| `-drivers` | `E2E_DRIVERS` | `3` | number of virtual drivers |
| `-ride-interval` | `E2E_RIDE_INTERVAL` | `5s` | base interval between rides per client (jittered ±50%) |
| `-shift-interval` | `E2E_SHIFT_INTERVAL` | `15s` | base interval between shift cycles per driver (jittered ±50%) |
| `-report-interval` | `E2E_REPORT_INTERVAL` | `10s` | stats report interval |
| `-run-for` | `E2E_RUN_FOR` | `0` (forever) | stop automatically after this duration — for timed/CI runs |
| `-seed` | — | wall clock | base random seed (per-actor seeds derive from it) |
| `-scenario` | `E2E_SCENARIO` | `` (continuous simulation) | run a one-shot proof instead — see "Scenario mode" below |

## What each actor does

**Client** (`client-N`): signup (`Client` role, unique per-run email) → login → attaches a billing payment method (`POST /payment-methods` with `pm_stub_ok`, op `billing.paymentmethod.add`, then GET-verified in the list, op `billing.paymentmethod.list`) → loop: `POST /request-ride` with randomized coordinates (occasionally against the `Standard USD` tariff — see below) → `GET /ride/{id}` asserting the full field round-trip, status `Requested`, no driver assigned, price > 0. Every 5th iteration checks the ride appears in `GET /ride` (via the shared admin token — see below); every 10th refreshes the access token. One dedicated `decline-client-0` runs the same loop but attaches `pm_stub_decline` instead, so its invoices are the ones that eventually go `uncollectible`.

**Driver** (`driver-N`): signup (`Driver` role) → login → `POST /driver` → GET-verify → loop: `POST /driver-shift/create` → GET-verify (`endedAt` empty) → `PUT /driver-shift/{id}` status `Online` (emits `shift.updated` through the outbox → Kafka) → work period, during which it polls `GET /drivers/{driverId}/offer` on matching-service every ~2s (op `matching.offer.get`) → status `Ended` → GET-verify `endedAt` set. Every 4th cycle updates the licence plate and verifies the round-trip (`driver.driver` no longer stores name/phone — see below).

When a poll during the work period finds an offer, the driver accepts it via `POST /rides/{rideId}/accept` (op `matching.ride.accept`) and deep-verifies: the offer disappears (`GET /drivers/{driverId}/offer` now 404s, another `matching.offer.get`) and a duplicate `POST /rides/{rideId}/accept` now 409s (op `matching.ride.accept.dup`). A 409 on the *first* accept just means another driver won the race for that ride; it's recorded under `matching.ride.accept` as a legitimate outcome, not a failure. After completing the ride, it polls `GET /rides/{rideId}/invoice` for up to ~30s for a terminal `paid`/`uncollectible` status (op `billing.invoice.get`; a still-`open` invoice after that window is normal async lag, not a failure) and, once terminal, calls `GET /ledger/balance` for that invoice's client/currency (op `billing.ledger.balance`) — the cheapest possible smoke test that the ledger query pipeline itself is healthy.

HTTP failures and verification mismatches are logged per actor and counted per operation in the report — they never kill the process, so the simulator can be started before/while the stack is coming up.

## Encoded service quirks

- `driver.shift` has no `status` column: only `"Ended"` persists anything (`ended_at`); other statuses just emit `shift.updated`. Shift end is verified via `endedAt`, never via a status round-trip.
- ride-service and driver-service still don't validate JWTs themselves — they trust Kong's injected `X-User-Id`/`X-Client-Id` headers, derived from the token's own claims (overwriting anything a caller sends), so every ride/driver/protected-auth call goes through `apiclient.bearerHeader` with the account's `accessToken` instead of a raw user ID. `ClientActor` keeps a second, genuinely signed-up `decoy` account specifically to exercise the "someone else can't cancel your ride" check, since a spoofed header no longer gets through.
- `ride.ride.client_id` is `auth.client(id)`, not `auth.user(id)` — a separate id from the account's own `userID`, only discoverable via `GET /me`'s `clientId` field. Every account fetches and caches it once at login (`account.clientID`); ride-request/read-back assertions compare against that, not `userID`.
- `GET /users`, `GET /driver`, and `GET /driver-shift` are Admin-only at the Kong gateway now (see `gateway/kong.yml`). The simulator logs in once at startup as the seeded admin (`services/shared/migrations/sql/0002_auth.up.sql`) and shares that token (`Deps.AdminAccessToken`) across every actor's list-endpoint verify — an ordinary client/driver token gets a 403 on these three routes.
- Emails embed a run ID (`client-{runID}-{i}@e2e.local`) because `auth.user.email` is unique and the DB persists across runs.
- ride-service doesn't consume `ride.accepted` yet, so once a driver accepts a ride the simulator has no way to verify that ride's Postgres status actually reaches `Matched` — that check lands once ride-service adopts Stage 2.

## Scenario mode

`-scenario=location-radius` (or `E2E_SCENARIO=location-radius`) replaces the continuous simulation above with a one-shot proof of the four behaviors location-service's Slice 1 exists to deliver (`docs/location/LOCATION_SPEC.md` §14), run entirely through the same public APIs a real client uses:

```bash
cd services/e2e-test
go run ./cmd -scenario=location-radius
```

It provisions its own drivers/clients at fixed, controlled positions (unrelated to the `-clients`/`-drivers` flags, which are ignored in this mode) and checks, for each:

1. **In-range vs. out-of-range** — a driver ~2km from a ride's pickup gets offered it; one ~10km away never does.
2. **Offline but pinging** — a driver who ends their shift (removed from matching-service's online pool) but keeps sending location pings is never offered, even though it's geographically in range the whole time.
3. **Radius expansion** — a driver ~8km away (outside attempts 1-2's 5km/7km, inside attempt 3's 9km) isn't offered until matching-service's retry has widened the search radius enough to include it.
4. **Staleness eviction** — a driver who stops pinging entirely is evicted from location-service's geo index and stops being offered rides, once its position is older than the staleness threshold.

Expect **~2.5-3 minutes total** — the staleness assertion can't resolve faster than location-service's own `LOCATION_STALENESS_SECONDS` default (120s) plus one sweep tick, and all four assertions run concurrently (not sequentially) specifically to keep that from dominating the whole run. Prints a PASS/FAIL line per assertion and exits non-zero if any failed.

For faster local iteration, temporarily lower `LOCATION_STALENESS_SECONDS`/`LOCATION_SWEEP_INTERVAL_SECONDS` on the running `location-service` container (e.g. to `20`/`5`) before running the scenario — this is a manual override for development, not something the scenario itself controls.

Since it runs on the host, it can't reach location-service's internal-only `GET /internal/drivers/nearby` (no Kong route — see CLAUDE.md's "Location service status") — every assertion is instead observed via `GET /drivers/{driverId}/offer`, the same endpoint the continuous driver actors already poll.

## Observing side effects

- Kafka UI (http://localhost:8080): `ride.requested` and `shift.updated` message counts grow while the simulator runs.
- `GET http://localhost:8090/api/ride/ride` (with a bearer token — see `POST /api/auth/login`) shows the simulated rides through the gateway.
