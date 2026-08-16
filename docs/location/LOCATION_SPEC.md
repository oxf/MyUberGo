# LOCATION_SPEC.md — Location Service

Design spec for `services/location-service` (Phase 3 of the README roadmap). Target port `:8004`.

Intended home: `docs/location/LOCATION_SPEC.md`, mirroring `docs/billing/BILLING_SPEC.md`.

**Status: Slice 1 complete (2026-08-12), Slice 2 not started.** This document was originally written in a chat session without codebase access. It has since been checked against the actual repo (2026-08-12) and corrected in place — see §0.1. Do not assume any table, topic, Redis key, contract struct, or container named here is present until its slice in §13 is checked off — check `services/shared/migrations/sql/`, `services/contracts/`, `gateway/kong.yml`, and `docker-compose.yml` before writing code against anything.

---

## §0. How to use this document

### §0.1 Corrections applied 2026-08-12 (repo-check pass)

The original draft got the *decisions* (§1–§3) right but several *repo-facing facts* wrong. Fixed in place, noted here so the reasoning isn't lost:

- **§13 replace directives**: `services/shared` is **not a Go module** (SQL migrations only, mounted into the `migrate` container). The three real directives are `contracts`, `observability`, `common`.
- **§15 env var names**: repo convention is `SERVICE_PORT` (not `PORT`), `REDIS_URL=redis:6379` (not `REDIS_ADDR`), `KAFKA_BROKER` singular (not `KAFKA_BROKERS`), one `PG_DSN` (not `POSTGRES_*`).
- **§6.1 migration number**: `0007_outbox_claimed_until` already exists; location's is `0008_location`. Its `outbox_message` table needs `claimed_until` and `trace_context` columns from day one, not just the ones shown — see the current `0006_billing.up.sql` for the up-to-date shape.
- **§7.1 paths**: Kong routes use `strip_path: true`, so service-side handlers register `/batch`, `/rides/{rideId}/counterparty`, etc. — not `/location/batch`.
- **§8.1**: `ride.started` **does exist** (`ride-service`'s `start_ride.go` publishes it via the outbox) — only `ride.finished` doesn't (the real completion event/topic is `ride.completed`). The conclusion (open the tracking window on `ride.accepted`, not `ride.started`) is unchanged; the reasoning is now "the passenger wants to watch the approach" rather than "the topic doesn't exist."
- **§2.8 Stage-2 shape**: `internal/workers/outbox_worker.go`, the CQRS decorators, health, shutdown, the tx manager, and the Kafka consumer loop have all moved into `services/common` since this spec's shape was described. A new service's `internal/common/*` and `internal/infrastructure/{health,shutdown,metrics}` are now ~10-line type-alias shims (see `matching-service`'s versions) — scaffolding is cheaper than the original text implies.
- **§11 gap**: `.github/workflows/ci.yml` runs a hardcoded matrix of service directories. A new service not added there is never built, vetted, tested, or linted — added as a required step.
- **§12 readiness**: `common/health.Checker` takes exactly **one** `Pinger`. Moot for Slices 1–2 (Redis only); a Postgres-backed Slice 3 needs a composite pinger, which has no precedent in this repo yet.
- **§7.3 danger, now fixed**: the original text said to reflect WS socket-pump goroutine health in the liveness checker "the way the other services do for workers." That would be a bug, not a feature — `health.GoSafe(logger, checker, workerCtx, name, fn)` flips the service to `Live: false` when `fn` returns before `workerCtx` is cancelled. A per-connection WS pump *legitimately* returns on every normal client disconnect. Wired like a worker, the first driver closing their app marks the whole service dead and Docker restarts it. **Per-connection pumps must pass `nil` for `workerCtx`**, exactly like the existing HTTP-server goroutine does in every service's `cmd/main.go` — only the sweeper, consumers, and the Slice-2 Pub/Sub dispatcher are real long-lived workers.
- **New, not in the original draft**: `obshttp.Handler` wraps handlers in `otelhttp`, which would hold one span open for a WebSocket connection's entire lifetime — `/ws` needs the same exclusion `/health/` already gets. `services/common` has no HTTP client package; §2.2's synchronous call is the repo's first service-to-service HTTP call, built as a new shared `services/common/httpclient` (timeout + otelhttp transport) since ride-service wants the same thing in Slice 4. §5.1's keys are actually prefixed `loc:` (see §17 decisions below) to avoid colliding with matching-service's `driver:*`/`ride:*` keys in the same shared Redis DB 0.

- §1–§3 are **decisions already made** with their rationale. Don't re-litigate them mid-implementation; if one turns out wrong, change it here first and note why.
- §4–§12 are the **design**: model, storage, contracts, integration.
- §13 is the **implementation order**. Work slice by slice. Each slice ends with a working, testable system.
- §16 lists **traps** that are known to bite in this exact design. Read it before Slice 1.
- §17 is **open questions** — decide with the repo owner rather than guessing.

Repo conventions that apply here as they do everywhere else:
- Update `PLAN.md` (dated section, what was done and why) and `CLAUDE.md` (only where current code diverges from README) as work lands. Update `README.md`'s Location Service section when it stops being aspirational.
- `go build ./... && go vet ./... && go test ./... && golangci-lint run` clean before calling a slice done.
- Extend `services/e2e-test` whenever an endpoint is added — it's the de facto integration test.

---

## §1. Scope

### In scope

1. Ingest position pings from drivers and clients (WebSocket primary, HTTP batch fallback).
2. Maintain live position + a geospatial index in Redis.
3. Serve nearby-driver discovery to `matching-service` (internal).
4. Stream counterparty position to the other party during an active ride (WS), authorized against ride state.
5. Archive raw pings to a time-series store; build a per-ride summary (map-matched polyline + distance) at ride end.
6. Proxy and cache Geoapify: forward/reverse geocoding, address autocomplete, routing/ETA, map matching.

### Explicitly out of scope

| Concern | Owner |
|---|---|
| Map rendering, tiles, marker UI | client + Geoapify directly |
| Driver online/shift state | `driver-service` |
| Ride state machine, who is matched with whom | `ride-service` (Location *consumes* these events) |
| Ranking / offer fan-out | `matching-service` |
| Push delivery of ride events | Notification service (unbuilt) |
| Surge/demand heatmaps, geofencing, zones | not planned |
| Driver payouts, fare adjustment logic | `billing-service` |

**Boundary rule:** Location owns *where*. `driver-service` owns *whether a driver is available*. `matching-service` owns *who gets offered a ride*. Location must never need to know shift state to accept a ping (see §2.3).

---

## §2. Decisions, with rationale

These were argued through before this spec was written. The rationale matters more than the conclusion — keep it when revisiting.

### §2.1 Location Service owns live position; matching does not ingest pings

**Decision:** pings arrive at Location. Matching queries Location for nearby candidates.

**Why not have drivers ping matching-service directly** (which is fewer moving parts, and matching already keeps its own Redis read model of `drivers:online`):
- Three consumers need this stream: matching (discovery), tracking (UC3/UC4), history (UC5, billing verification). If matching is the ingest point, the other two must read out of matching's Redis, and matching quietly becomes a location service.
- Ingest and matching have **opposite operational profiles**: ingest is constant/high-volume/loss-tolerant; matching is bursty/low-volume/loss-intolerant. Coupling them means a ping storm degrades ride dispatch.

**Ownership ≠ exclusive storage.** Matching keeping a derived projection would be fine (it already does this with `shift.updated`). What's being avoided is matching being the place pings *arrive*.

### §2.2 Discovery is a synchronous internal HTTP call, with degradation

**Decision:** `matching-service` calls `GET /internal/drivers/nearby` on Location, **over-fetches**, and intersects locally against its own `drivers:online` ZSET.

**Why over-fetch + local intersect:** Location doesn't know shift state (§2.3), so it returns geographic candidates only. Matching already owns availability. Two services, two rules, one owner each.

**Why not Kafka fan-out of per-ping events** (which would keep matching fully autonomous, consistent with the `shift.updated` pattern): ~200 msg/s of 60-byte payloads with a 10-second shelf life and one interested reader. Kafka's durability and replay guarantees are wasted on data that is worthless 10 seconds after it's produced. Revisit only if the sync dependency proves painful.

**Degradation (must be implemented, not assumed):** if Location is unreachable or returns an error, matching falls back to today's rating-only pool from `drivers:online`. Non-optimal matches beat no matches. Log + metric the fallback; do not fail the ride.

### §2.3 Ingest accepts every authenticated ping; the online-filter lives on the read path only

**Decision:** Location does not check shift state at ingest.

**Why:** the filter already exists at the read path (§2.2). Enforcing it at ingest too means the same rule lives in two services against two copies of shift state, which can disagree — producing a driver who is online in matching but invisible in the geo index, with no obvious place to look. One rule, one owner.

**Explicitly not the reason:** cost. A Redis lookup per ping is ~0.2% of one Redis instance at target scale. Performance was never the argument.

**Accepted consequence:** the geo index holds every recently-pinging driver, not just online ones. Over-fetch a multiple of the desired count to compensate. At ≤1k drivers this is a non-issue.

### §2.4 Upfront pricing stays authoritative; history is not a system of record

**Decision:** the quoted price at ride request remains binding. Location publishes actual distance/duration; `billing-service` **records** it on the invoice alongside quoted, but does **not** adjust the charge.

**Why:** matches industry standard (Uber/Bolt/Lyft quote upfront; the platform absorbs route risk and wins on average). Near-zero disputes, better conversion.

**The architectural consequence is the important part:** because actual distance is not authoritative for money, **the raw history store is an audit/verification input, not a system of record.** It may be eventually consistent, lossy under load, and aggressively TTL'd. This removes the main objection to a NoSQL store (§6.2).

Deferred, additive later: mid-ride re-pricing when actual materially exceeds quoted (typical threshold 20–25%), destination changes, added stops, waiting time. Out of scope for this spec.

### §2.5 Summary is built by map matching, not by sampling

**Decision:** at ride end, feed raw pings to Geoapify Map Matching → road-snapped polyline + road distance. Store as an **encoded polyline string**.

**Why not "keep every Nth ping":** every-Nth is the worst available downsampling rule. It keeps redundant points on straight stretches and deletes information-carrying ones. A 90° turn falling between kept pings vanishes, and the receipt map shows a car driving through a building. Sampling by *time* discards by *geometry*.

**Fallback if the Geoapify call fails:** Ramer–Douglas–Peucker simplification of the raw track (keeps points by geometric significance) + Haversine sum for distance. Mark the summary row's `source` accordingly so a degraded summary is distinguishable.

**Why encoded polyline, not a JSON array of points:** ~5–8 bytes/point vs ~40, decodable natively by every mapping library, and the client would have to convert an array anyway.

### §2.6 Three storage tiers, three stores

| Tier | Content | Store | Retention | Serves |
|---|---|---|---|---|
| Hot | current position, geo index | Redis | TTL minutes | matching, live tracking |
| Raw | every accepted ping | NoSQL | 7–30d TTL | disputes, safety, replay, debugging |
| Summary | one row per ride | **Postgres** | long | receipts, past-ride map, billing verification |

The summary is ~1 row per ride — thousands, not millions. It joins to `ride.ride` and `billing.invoice`. It belongs in Postgres. Do not put it in the NoSQL store just because it is location-shaped.

### §2.7 WebSocket from day one, HTTP batch alongside it

WS is a deliberate learning goal for this project, not a requirement derived from the load. Accepted — with two conditions:

1. Ingest is defined as a **port** (`LocationIngestor`) with two adapters: WS and `POST /api/location/batch`. The HTTP adapter is ~20 lines, lets `e2e-test` drive pings without a WS client, and is the real-world fallback for devices on networks where WS won't stay up.
2. WS *push* (server → client) is the genuinely hard half and must be designed for multi-instance from the start — see §7.3.

### §2.8 Stage 2 from day one

`PLAN.md` says new services start at Stage 1. `billing-service` went Stage 2 from day one instead, and that was right.

**Recommendation: Stage 2 from day one here too.** The Stage-1 learning value (see how procedural code hurts) has been extracted five times already. This service has real novelty — geospatial indexing, WS lifecycle, a new datastore — and spending the first pass rediscovering layering dilutes it.

Copy `driver-service`/`billing-service` shape: `domain` / `application/{command,query,services}` / `common/{decorator,errors}` / `persistence` / `infrastructure` / `interfaces/http/handler` / `workers` / `cmd/main.go` as composition root. Handlers constructed via `NewXHandler(...)` wrapped immediately with `decorator.ApplyCommandDecorators` / `ApplyQueryDecorators`.

**This one is the repo owner's call, not Claude Code's** — confirm before scaffolding.

---

## §3. Assumptions and targets

Write these into `PLAN.md`. They are the reason half the "obvious" scaling work below is deliberately absent.

| | Target |
|---|---|
| Concurrent online drivers | ≤ 1,000 |
| Ping interval, on-ride | 3–5 s |
| Ping interval, idle/online | 10–15 s |
| Peak ingest | ~200 writes/s |
| Freshness, live tracking | ≤ 5 s |
| Freshness, matching index | ≤ 15 s |
| Staleness eviction | 120 s without a ping |
| Discovery radius | 5 km default, expanding per matching's retry |
| Raw history volume | ~5–17M pings/day at full load |

**A single Redis instance handles ~100k ops/s.** At ~600 ops/s (3 writes/ping) this design uses well under 1% of one Redis. Sharding, geohash bucketing by city, PostGIS, and read replicas are all **premature** at this scale — noted here so they can be rejected with a number rather than a feeling.

Idle drivers ping at a lower rate than on-ride drivers on purpose: the matching index does not need 1 Hz precision, and this is where most of the battery and mobile-data budget is saved.

---

## §4. Domain model

```
Position          lat, lon (float64), accuracyM, headingDeg, speedMps, deviceTs, serverTs
Ping              subjectType (Driver|Client), subjectId, rideId (nullable), Position
TrackingWindow    rideId, clientUserId, driverId, openedAt, closedAt   // authorization scope
RideTrack         rideId, ordered []Position                            // raw, for archive/summary
RideSummary       rideId, start/end Position, polyline, distanceM, durationS, source
```

**Value objects worth making real types** (not bare floats): `Coordinate` with construction-time validation, and `DistanceMeters` as `int64`. Rationale mirrors the repo-wide money convention — do fractional maths in `float64`, round **once** via `math.Round`, persist `int64`. Never persist an intermediate float distance.

**Coordinates stay `float64` everywhere.** `float32` gives ~1 m of representational error, which silently corrupts short-distance comparisons.

---

## §5. Redis design (hot tier)

### §5.1 Keys

**All keys are prefixed `loc:`** (added 2026-08-12; not in the original draft). Redis DB 0 is shared with matching-service (`driver:*`, `ride:*`, `drivers:online`) and Kong's rate-limiter, with no per-service namespace convention already in place — an unprefixed `driver:{id}` here would collide with matching-service's own hash of the same name (see §9).

| Key | Type | Contents | TTL |
|---|---|---|---|
| `loc:drivers:geo` | GEO (ZSET) | driverId → geohash score | none (swept, see §5.3) |
| `loc:drivers:lastseen` | ZSET | driverId → unix ms | none (swept) |
| `loc:driver:{id}` | HASH | lat, lon, accuracyM, headingDeg, speedMps, deviceTs, serverTs | 5 m |
| `loc:driver:{id}:owner` | STRING | userId, cached from `shift.updated` | 7 d |
| `loc:user:{userId}:driver` | STRING | driverId, reverse of the above (§9) | 7 d |
| `loc:client:{userId}` | HASH | same shape as `loc:driver:{id}` | 5 m |
| `loc:ride:{id}:participants` | HASH | clientUserId, driverId, openedAt | 24 h |
| `loc:ride:{id}:track` | STREAM | capped ping buffer for archival | 24 h |
| `loc:geo:fwd:{sha256(q\|lang\|bias)}` | STRING | Geoapify forward-geocode JSON | 7 d |
| `loc:geo:rev:{lat5}:{lon5}` | STRING | Geoapify reverse-geocode JSON | 30 d |
| `loc:geo:ac:{sha256(q\|lang\|bias)}` | STRING | autocomplete JSON | 1 d |

Clients are **not** in the geo index — nobody searches for nearby passengers. A hash lookup is sufficient for UC4.

`loc:ride:{id}:track` as a capped Redis Stream (`XADD ... MAXLEN ~ 5000`) rather than a LIST: ordered, range-readable by id, trims itself, and survives a slow archiver without unbounded growth.

### §5.2 Redis GEO — what to know before writing the code

A geo key **is** a sorted set whose score is a 52-bit interleaved geohash. Bits of lat and lon are interleaved (`b₁a₁b₂a₂…`) so physically close points share a long numeric prefix — 2D proximity becomes 1D adjacency. Consequences:

- `ZRANGE`/`ZREM`/`ZCARD` work on it. There is no `GEODEL`; you remove with `ZREM`.
- `GEOADD key lon lat member` — **longitude first.** This is the single most common bug in geo code.
- Use `GEOSEARCH ... FROMLONLAT lon lat BYRADIUS 5 km ASC COUNT n WITHDIST`. `GEORADIUS` is deprecated since Redis 6.2. `BYBOX` is cheaper than `BYRADIUS` and matches the README's "bounding box" wording.
- Distances are computed on a sphere, not an ellipsoid — up to ~0.5% error. Irrelevant for dispatch; relevant if it ever touches money (it doesn't, per §2.4).
- One key = one Redis Cluster slot. The whole index lives on one node. Fine at target scale; the answer for later is sharding by city (`loc:drivers:geo:{limassol}`).

### §5.3 Staleness sweeper — required in Slice 1

Redis GEO has **no per-member TTL** (the score is the geohash; it can't also encode expiry). A TTL on `loc:driver:{id}` does *not* remove that driver from `loc:drivers:geo`. Without a sweeper, a driver who force-kills the app stays in the index forever and keeps getting offered rides.

`StalenessWorker` — ticker + `select` loop, same shape as the existing outbox workers:
1. `ZRANGEBYSCORE loc:drivers:lastseen 0 <now-120s>` → stale ids
2. `ZREM loc:drivers:geo <ids...>` and `ZREM loc:drivers:lastseen <ids...>` in one pipeline
3. Emit a counter metric for evictions

Run every 30 s. Belt-and-braces: also filter query results by `lastseen` on read, so a sweeper outage degrades rather than breaks correctness.

### §5.4 The two-key problem

`matching-service`'s existing `drivers:online` ZSET stores **rating** as the score. A geo key must store the **geohash** as the score. They cannot be the same key — `GEOADD` into `drivers:online` would destroy every rating.

They also live in different services now. Location owns `loc:drivers:geo`; matching keeps `drivers:online`. The intersection happens in matching's application layer after the internal HTTP call. Do not attempt to merge them in Redis.

### §5.5 Ping validation and ordering

Reject (count by reason, don't just drop silently):

- lat outside [-90, 90], lon outside [-180, 180], or NaN/Inf
- `accuracyM` > 100 (configurable) — a 500 m-accuracy fix is noise
- `deviceTs` more than 2 min in the future or 10 min in the past (clock skew / replay)
- implied speed from the previous accepted position > 200 km/h — catches teleports and naive GPS spoofing

**Ordering:** buffered pings replayed after a tunnel arrive out of order. Order by `deviceTs`, not server arrival, and skip a write whose `deviceTs` is older than the stored one. Device clocks are wrong sometimes; the validation window above bounds the damage. Last-write-wins without a Lua script is acceptable at this scale — **document that choice in code** rather than leaving it implicit.

---

## §6. Persistent stores

### §6.1 Postgres — `location` schema

**Not needed until Slice 3.** Slices 1–2 are Redis + Kafka only, same shape as matching-service — no `PG_DSN`, no `migrate` dependency in `docker-compose.yml`.

When Slice 3 lands: new migration pair `services/shared/migrations/sql/0008_location.{up,down}.sql` — `0007_outbox_claimed_until` is the current highest number (added 2026-08-08, retrofits `claimed_until` onto every existing outbox table). Check the highest existing number again before creating this file; do not renumber existing ones.

```
location.ride_summary
  ride_id           UUID PRIMARY KEY          -- FK to ride.ride deferred; cross-schema FK is optional, follow 0005's precedent
  client_id         UUID NOT NULL
  driver_id         UUID NOT NULL
  started_at        TIMESTAMPTZ NOT NULL
  ended_at          TIMESTAMPTZ NOT NULL
  start_lat         DOUBLE PRECISION NOT NULL
  start_lon         DOUBLE PRECISION NOT NULL
  end_lat           DOUBLE PRECISION NOT NULL
  end_lon           DOUBLE PRECISION NOT NULL
  polyline          TEXT NOT NULL             -- encoded polyline, precision 5
  distance_m        BIGINT NOT NULL           -- integer metres, never float
  duration_s        INT NOT NULL
  point_count       INT NOT NULL              -- raw pings that fed this summary
  source            TEXT NOT NULL CHECK (source IN ('MapMatched','Simplified'))
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()

location.outbox_message
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4()
  topic             TEXT NOT NULL
  event_type        TEXT NOT NULL
  payload           JSONB NOT NULL
  created_at        TIMESTAMPTZ DEFAULT NOW()
  processed         BOOLEAN DEFAULT FALSE
  retries           INTEGER DEFAULT 0
  claimed_until     TIMESTAMPTZ               -- lease for common/outbox.Worker's claim-batch step; every
                                               -- outbox table has this column now, not just ride's original one
  trace_context     JSONB                     -- see services/observability/obsoutbox; must be inserted as
                                               -- `var traceContext any`, never a nil []byte (CLAUDE.md's
                                               -- outbox-trace-continuity note explains why the cast fails otherwise)
```

README calls this `RIDE_SUMMARY_LOCATION`; `location.ride_summary` is more consistent with `ride.ride` / `driver.driver`. Update the README when it lands.

### §6.2 NoSQL — raw ping history

**Recommendation: DynamoDB Local**, added to `docker-compose.yml`.

- Modelling: `PK = subject_id`, `SK = timestamp_ms`, GSI on `ride_id` (README's design, and it's correct). TTL attribute, 30 days.
- Chosen over Cassandra mostly on laptop weight (~500 MB vs ~2 GB heap) and setup friction. ScyllaDB is the lighter Cassandra option if wide-column modelling specifically is the learning goal.

**Be honest in `PLAN.md`:** at ≤1k drivers this is *chosen for learning value, not scale necessity*. A partitioned Postgres table with a BRIN index on timestamp would handle this workload fine. That's a legitimate reason to do it — the failure mode to avoid is writing a post-hoc justification pretending it was needed.

**Required regardless of store:** define `domain.LocationHistoryRepository` as a port. The store is the most likely thing in this service to be swapped.

**Write path:** batch. Do not write one row per ping synchronously on the ingest path — buffer in the Redis stream and drain with an `ArchiveWorker` on a ticker, or write batched at ride end. Ingest latency must not depend on the archive store being healthy.

---

## §7. Interfaces

### §7.1 HTTP — client-facing (via Kong, `/api/location`)

Kong's `/api/location` route uses `strip_path: true` (same as every other service's route in `gateway/kong.yml`), so these are the **service-side** paths — the client-facing path is `/api/location` + this column, e.g. `POST /api/location/batch`. Don't register `/location/batch` inside the service itself; that would double the prefix.

| Method | Service path | Client-facing path | Auth | Purpose |
|---|---|---|---|---|
| `POST` | `/batch` | `/api/location/batch` | any authenticated | ping fallback / e2e-test driver |
| `GET` | `/rides/{rideId}/counterparty` | `/api/location/rides/{rideId}/counterparty` | participant only | one-shot position (UC3/UC4 without WS) |
| `GET` | `/rides/{rideId}/track` | `/api/location/rides/{rideId}/track` | participant or Admin | summary polyline for a past ride |
| `GET` | `/geocode?text=` | `/api/location/geocode?text=` | any authenticated | Geoapify forward geocode, cached |
| `GET` | `/geocode/reverse?lat=&lon=` | `/api/location/geocode/reverse?lat=&lon=` | any authenticated | reverse geocode, cached |
| `GET` | `/autocomplete?text=` | `/api/location/autocomplete?text=` | any authenticated | address autocomplete, cached |

### §7.2 HTTP — internal (not routed through Kong)

| Method | Path | Caller | Purpose |
|---|---|---|---|
| `GET` | `/internal/drivers/nearby?lat=&lon=&radiusKm=&limit=` | matching-service | geographic candidates + distances |
| `GET` | `/internal/distance?fromLat=&fromLon=&toLat=&toLon=` | ride-service | route distance/ETA for fare estimate |

Do **not** add a Kong route for `/internal/*`. These are Docker-network-only, same trust model as the rest of the repo: Kong is the sole ingress, so anything without a Kong route is unreachable from the host. Do not publish location-service's port in `docker-compose.yml`.

`nearby` returns geographic candidates only — no availability filtering, no ranking (§2.2, §2.3).

### §7.3 WebSocket

`GET /api/location/ws` (upgrade), through Kong on the protected tier.

**Client → server:** `LocationUpdate` frames, one or batched.
**Server → client:** `CounterpartyLocationUpdate` frames during an open tracking window.

Lifecycle rules:

- **Authorize at connect time.** Kong validates the JWT on the *upgrade request only* — after the upgrade there is no per-message JWT check and no Kong rate limiting. Bind the connection to the identity from `X-User-Id` / `X-Client-Id` at handshake and never trust a self-asserted id in a frame body.
- **Per-connection ping-rate cap in-process** (token bucket, e.g. 2/s burst 10). Kong cannot do this for WS messages. Must-have, not nice-to-have.
- **Close sockets when the window closes.** The `ride.completed` / `ride.cancelled` consumer must terminate the participants' tracking subscriptions. Otherwise a socket opened during a valid ride streams forever.
- **Heartbeat.** Kong's default `proxy_read_timeout` (60 s) will kill an idle WS. Send WS ping frames every ~25 s and/or raise the timeout on the route. Verify against `gateway/kong.yml`.
- **Graceful shutdown.** Every service in this repo drains HTTP properly; long-lived sockets need an explicit drain phase or every deploy hard-kills every driver's connection. Wire into the existing shutdown machinery, and reflect socket-pump goroutine health in the liveness checker the way the other services do for workers.

**Multi-instance fan-out — the actual hard part.** The passenger's socket may live on instance A while the driver's ping arrives at instance B. B cannot reach A directly. Design for this from the start:

- Publish accepted positions to Redis Pub/Sub channel `loc:ride:{rideId}:positions`.
- Every instance subscribes for rides it currently has a socket for, and forwards to its local sockets.
- Single-instance dev works identically, so there's no special case to unwind later.

This is the distributed-systems lesson in this service. Don't route around it with sticky sessions unless you decide to, deliberately, and write down why.

### §7.4 Contracts

- `services/contracts/http/location-service.go` — request/response DTOs, mirroring the existing per-service files.
- `services/contracts/kafka/location-service.go` — event payloads.
- `web/src/api/types.ts` — mirror any DTO the dashboard consumes.

Distances follow the money-convention discipline: integer metres, field named `distanceM`, never a bare float.

---

## §8. Kafka integration

### §8.1 Consumed

| Topic | Exists today? | Action |
|---|---|---|
| `ride.accepted` | ✅ yes | open tracking window: write `loc:ride:{id}:participants`, start `loc:ride:{id}:track` |
| `ride.completed` | ✅ yes | close window, archive, build summary, publish, clean up Redis |
| `ride.cancelled` | ✅ yes | close window, archive, clean up; summary only if the ride actually started |
| `shift.updated` | ✅ yes | cache both `loc:driver:{id}:owner` → `userID` and `loc:user:{userId}:driver` → `driverID` (§9) |

**Deliberately not consuming `ride.started`, even though it exists.** (Correction, 2026-08-12: `ride-service` does publish it — `internal/application/command/start_ride.go` — the original "it doesn't exist yet" premise was wrong. Only `ride.finished` is actually missing; the real completion topic/event is `ride.completed`.) Opening the window on `ride.accepted` instead is still the right call on its own merits — the passenger wants to watch the driver approach, which happens *between* accept and start, and `ride.accepted` is already consumed by ride-service and driver-service for their own state transitions, so Location joins an existing, proven at-least-once path rather than a new one.

Consumer groups: `location-service`. Same reader-loop shape as the existing consumers. Handlers must be idempotent against redelivery (guard on window already open / summary already exists).

### §8.2 Published

`ride.summary.ready` — `{rideId, clientId, driverId, distanceM, durationS, polyline, startedAt, endedAt, source}`

Published via the **transactional outbox** pattern (`location.outbox_message` + `OutboxWorker`), same as ride/billing. The summary write and the event publish are a dual write into Postgres, which is exactly what the outbox exists for.

Consumers: `billing-service` records actual-vs-quoted on the invoice (§2.4 — records, does not re-price). Notification service later.

**Do not publish per-ping location to Kafka** (§2.2).

---

## §9. Authorization

Kong authenticates; Kong does **not** know ride participation. Ownership is enforced in-service — the same pattern `matching-service` uses for driver ownership and `billing-service` uses for invoice ownership.

- **Tracking window as the authorization scope.** `loc:ride:{id}:participants` is written on `ride.accepted` and deleted on `ride.completed`/`ride.cancelled`. A read of counterparty position is permitted **only** when the caller's `X-User-Id`/`X-Client-Id` is in that hash. Window closed → 403, regardless of history.
- **Driver identity at ingest — resolved, not asserted** (changed 2026-08-12 from the original self-assert-and-verify design). A ping payload carries **no `driverId` field at all**. The `shift.updated` consumer (§8.1) caches the mapping in both directions — `loc:driver:{id}:owner` → `userID` (mirroring matching-service's `domain.DriverRepository.GetUserID` technique) **and** `loc:user:{userId}:driver` → `driverID`. Ingest looks up the caller's `driverId` from Kong's injected `X-User-Id` via the reverse key; no cached mapping means the caller has never opened a shift, and is `ErrForbidden`/403 — the same "unknown driver is a deny" rule matching-service already applies, just phrased as "unknown user" here since the user is what Kong actually authenticates. This removes an entire class of bug (an ownership check that's present but wrong) by removing the field it would have checked. Do **not** mint a `driver_id` JWT claim: that would make auth-service query `driver.driver`, a table it doesn't own, breaking the per-service schema-ownership convention. This was already decided once in this repo; don't re-decide it differently.
- **Historical track reads** (`/rides/{rideId}/track`): the ride's own client or driver, or an Admin. This one outlives the window by design — it reads the summary, not live position.
- **`/internal/*` has no user auth** — it's network-isolated and callers are services. Do not add a Kong route for it.

**Privacy note (real, not theoretical — this project is EU-based):** position data is personal data. The TTLs in §5/§6 are the retention policy, and they need to actually work. A right-to-erasure request must be satisfiable: raw history is keyed by `subject_id`, so deletion is a partition drop; the summary polyline is arguably pseudonymised but tied to `client_id`. Worth one paragraph in the spec when GDPR handling is actually implemented — out of scope for the slices below, but don't design a store that makes it impossible.

---

## §10. Geoapify integration

One vendor, six APIs: forward geocode, reverse geocode, autocomplete, routing, route matrix, map matching. EU-based (GDPR-friendly), OSM-backed. Credit-based pricing with a free tier — **verify current limits before relying on them**; those numbers move.

**Port it.** `domain.GeocodingProvider`, `domain.RoutingProvider`, `domain.MapMatchingProvider` (or one `GeoProvider` if that stays cohesive). Adapter in `internal/infrastructure/geoapify`. Vendors get swapped; this is cheap now and expensive later. `billing-service`'s `PaymentProvider` port is the reference for how well this pays off.

**Caching is the point of proxying** (§5.1 keys):
- Normalize before hashing the cache key: trim, lowercase, collapse whitespace, include language and location bias.
- Reverse geocode: round coordinates to 5 decimals (~1.1 m) before keying, or the hit rate is zero.
- Emit a cache-hit-ratio metric. If it's low, the normalization is wrong.

**Resilience:**
- Per-user rate limit on the proxy endpoints — otherwise one buggy client burns the shared quota. This is the concrete risk of proxying rather than letting clients call directly with their own restricted key.
- Timeout (2 s), retry once, then circuit-break. A Geoapify outage must not take down ingest or discovery — those paths don't touch it.
- API key from env, never in code or contracts.

**Known weak spot (assumption, unverified):** OSM-based routing is generally weaker than Google on live traffic-aware ETAs. Irrelevant here; it's the first thing you'd swap in production.

**Autocomplete** lives in this service for now. It doesn't really belong — client-facing, latency-critical, no ride context, different cache profile — but there's no BFF and won't be. Keep it in its own package behind its own port so extracting it later is a file move, not a refactor.

---

## §11. Changes required in other services

Not optional — Location is useless without these.

**`matching-service`:**
- New `LocationClient` port + HTTP adapter calling `GET /internal/drivers/nearby`, built over a new shared `services/common/httpclient` (timeout + otelhttp transport) — this is the first outbound HTTP call this service has ever made, and the first HTTP client anywhere in `services/common`.
- Discovery: call it, over-fetch (`limit = desired × 5`), intersect with `drivers:online` (via a new `DriverRepository.OnlineRatings(ids)` pipelined-`ZSCORE` method — `TopOnlineDrivers` alone can't answer "are these specific IDs online"), then rank. **Decided (2026-08-12): `0.5×distance + 0.5×rating`**, not README's `0.4/0.4/0.2` — `acceptance_rate` has no data source anywhere in the repo, so the weight is redistributed across the two signals that actually exist rather than hardcoding the missing term to a constant (which would just be the same ordering with extra code). Revisit the split once acceptance tracking exists.
- Retry becomes **radius expansion** (5→7→9→11→13 km, cap 15) as the README always intended, replacing today's pool-widening workaround.
- **Fallback path (§2.2):** on Location error/timeout, use today's rating-only pool. Metric it via `myubergo.matching.location_fallbacks`.

**`ride-service`:** optionally call `/internal/distance` for fare estimation instead of straight-line. Additive; don't break the existing estimator in the same pass.

**`billing-service`:** consume `ride.summary.ready`, store `actual_distance_m` / `actual_duration_s` on the invoice. Record only — no re-pricing (§2.4).

**`gateway/kong.yml`:** `/api/location` route on the protected tier (`jwt` + `inject_user_headers`), plus WS timeout config on that route. `kong config parse` against the pinned image before committing, as with the matching route.

**`docker-compose.yml`:** location-service (no host port), DynamoDB Local, both in Kong's `depends_on` where appropriate.

---

## §12. Observability

Everything goes through the existing OTel pipeline; handlers get the standard logging/metrics/tracing decorators. Service-specific signals:

| Signal | Type | Why |
|---|---|---|
| ingest lag (`serverTs − deviceTs`) p50/p95/p99 | histogram | the freshness SLI; the number §3 promises |
| pings accepted / rejected **by reason** | counter | validation tuning; spoofing detection |
| active WS connections | gauge | capacity, and leak detection on close bugs |
| geo index size vs `drivers:online` size | gauge | divergence = sweeper or event-consumer bug |
| staleness evictions | counter | sweeper is alive and doing work |
| `nearby` query latency + candidates returned | histogram | zero-candidate rate is a matching-health leading indicator |
| Location-unavailable fallbacks in matching | counter | §2.2's degradation path must be visible when it fires |
| Geoapify calls, cache hit ratio, errors, circuit state | counter/gauge | cost control |
| summary build duration and failures by source | histogram/counter | `Simplified` rate = map-matching health |

Health: readiness checks Redis + Postgres + history store. Liveness reflects real worker-goroutine state (sweeper, archiver, outbox, WS pumps) — the repo already fixed the `defer ticker.Stop()` bug across all services; don't reintroduce that shape.

---

## §13. Implementation slices

Each slice ends with something that builds, runs, and is exercised by `e2e-test`.

### Slice 1 — Ingest + geo index + discovery *(must have)*

Unblocks matching's geo discovery, which is the highest-value open item in the backlog.

- [x] Scaffold `services/location-service` (Stage 2 shape per §2.8), own Go module, `replace` directives for `contracts`/`observability`/`common` (**not** `shared` — that directory has no `go.mod`), Dockerfile copying the whole service dir (not just `cmd` — see the auth-service Dockerfile bug in `CLAUDE.md`)
- [x] `LocationIngestor` port; `POST /batch` adapter first (testable without a WS client) — no `driverId` in the request body (§9)
- [x] Ping validation (§5.5) with per-reason rejection metrics
- [x] `GEOADD loc:drivers:geo` + `ZADD loc:drivers:lastseen` + `loc:driver:{id}` hash, pipelined
- [x] `StalenessWorker` (§5.3) — **this slice, not later**
- [x] `GET /internal/drivers/nearby` via `GEOSEARCH ... WITHDIST`
- [x] Kong route (`strip_path: true`, matching-service's route is the template), docker-compose entry (no host port, Redis + Kafka deps only — no Postgres until Slice 3), health checks (Redis-only pinger), graceful shutdown
- [x] `shift.updated` consumer → cache the owner mapping **both directions** (`loc:driver:{id}:owner`, `loc:user:{userId}:driver`) for §9
- [x] new `services/common/httpclient` (timeout + otelhttp transport) — this is matching-service's first outbound HTTP call ever, so there's no existing client helper to reuse
- [x] matching-service: `LocationClient` over `httpclient`, over-fetch + intersect via a new `OnlineRatings(ids)` repo method, `0.5×distance + 0.5×rating` ranking (§17 decision — README's 0.2 acceptance weight has no data source yet), radius-expanding retry (5→7→9→11→13km, cap 15), **fallback to today's `TopOnlineDrivers` pool on Location failure**, `myubergo.matching.location_fallbacks` counter
- [x] `.github/workflows/ci.yml`: add `location-service` to the build/vet/test/lint matrix
- [x] `e2e-test`: drivers emit pings (§14) with a bearing-and-speed movement model; assert the four behaviours in §14

### Slice 2 — WebSocket + live tracking *(must have)*

- [ ] WS upgrade endpoint, connect-time auth binding, in-process rate cap
- [ ] WS ingest adapter behind the same `LocationIngestor` port
- [ ] `ride.accepted` consumer → open tracking window
- [ ] `ride.completed` / `ride.cancelled` consumers → close window, terminate sockets
- [ ] Redis Pub/Sub fan-out `loc:ride:{id}:positions` (§7.3) — multi-instance from the start
- [ ] `GET /rides/{rideId}/counterparty` one-shot fallback
- [ ] Heartbeat + Kong WS timeout config + WS-aware graceful shutdown

### Slice 3 — History + summary *(nice to have)*

- [ ] DynamoDB Local in compose; `LocationHistoryRepository` port + adapter; TTL configured
- [ ] `loc:ride:{id}:track` Redis Stream; `ArchiveWorker` batch drain
- [ ] `0007_location` migration: `ride_summary` + `outbox_message`
- [ ] Map-matched summary build at ride end, RDP + Haversine fallback, `source` recorded
- [ ] `ride.summary.ready` via outbox worker
- [ ] `GET /rides/{rideId}/track`
- [ ] billing-service consumes and records actual (no re-pricing)

### Slice 4 — Geoapify proxy *(nice to have)*

- [ ] Provider ports + Geoapify adapter, key from env
- [ ] Geocode / reverse / autocomplete endpoints with Redis caching + normalization
- [ ] `GET /internal/distance`; ride-service optionally adopts it
- [ ] Rate limiting, timeout, circuit breaker, cost metrics

### Explicitly deferred (premature at target scale)

Sharding the geo index by city; H3 instead of geohash; a separate read-model service; per-message WS JWT re-validation; mid-ride re-pricing; GDPR erasure tooling; surge/heatmaps; snap-to-road on the *live* path (summary only).

---

## §14. e2e-test simulator

The simulator is the only integration coverage in this repo, so ping emission ships in Slice 1.

**Movement model.** A random walk per axis is not good enough: displacement grows with √n, so a "driving" driver ends up ~40 m from the start after ten minutes, never entering or leaving any radius. Instead: give each driver a **bearing and speed**, advance along it each tick, apply a small random turn occasionally.

**Step size — the arithmetic:**

| Quantity | Value |
|---|---|
| 1° latitude | ≈ 111,320 m |
| 1° longitude at 34.7°N (Limassol) | ≈ 91,600 m |
| 0.00001° lat | ≈ 1.1 m |
| 50 km/h × 5 s = 69 m | ≈ **0.00062° lat** |

So the per-tick step is ~`6e-4` degrees, not `1e-8` (which is ~1.1 **millimetres** — below GPS noise, and would make every geo test pass without testing anything).

**Divide the longitude step by cos(lat)**, or eastbound drivers move ~1.2× faster than northbound ones at this latitude.

**Seed drivers in a ~10 km box around one city.** Uniformly scattered drivers mean a 5 km query returns nothing, matching always falls through to retry-and-fail, and the happy path is never covered. A bounded box gives you drivers both inside and outside the radius — which is the contrast the assertions need.

**Assertions (these are what make it a test rather than a demo):**

1. A driver inside the radius is offered the ride; one outside is not.
2. A driver who **stops pinging** disappears from candidates within the staleness window — catches a broken sweeper, otherwise invisible until production.
3. An **offline** driver who keeps pinging is never offered — catches a broken read-path intersection, the specific risk taken on by §2.3.
4. With no drivers nearby, radius expansion triggers rather than failing immediately.

Assertions 2 and 3 exist *because of* decisions §5.3 and §2.3. Tests written against decisions outlive tests written against code.

---

## §15. Configuration

Corrected 2026-08-12 to match repo convention (verified against `docker-compose.yml`'s other 5 Go services) — the original names (`PORT`, `REDIS_ADDR`, `POSTGRES_*`, `KAFKA_BROKERS`) don't match anything else in the repo and would have been the first divergent service.

| Env var | Default | Notes |
|---|---|---|
| `SERVICE_PORT` | `8004` | not `PORT` |
| `REDIS_URL` | `redis:6379` | not `REDIS_ADDR`; shared instance, as matching-service uses |
| `PG_DSN` | | Slice 3 only, once the `location` schema exists; not `POSTGRES_*` |
| `KAFKA_BROKER` | `kafka:29092` | singular, not `KAFKA_BROKERS`; consumer group `location-service` |
| `DYNAMODB_ENDPOINT` | | Slice 3 only, local endpoint in compose |
| `GEOAPIFY_API_KEY` | | Slice 4 only |
| `LOCATION_STALENESS_SECONDS` | `120` | sweeper threshold |
| `LOCATION_SWEEP_INTERVAL_SECONDS` | `30` | |
| `LOCATION_MAX_ACCURACY_M` | `100` | validation |
| `LOCATION_MAX_SPEED_KMH` | `200` | teleport/spoof rejection |
| `LOCATION_MAX_FUTURE_SKEW_SECONDS` | `120` | reject a ping whose `deviceTs` is this far in the future |
| `LOCATION_MAX_PAST_SKEW_SECONDS` | `600` | reject a ping whose `deviceTs` is this far in the past |
| `LOCATION_WS_PING_SECONDS` | `25` | Slice 2; under Kong's 60 s read timeout |
| `LOCATION_HISTORY_TTL_DAYS` | `30` | Slice 3 only |

---

## §16. Known traps

Every one of these has a specific failure mode; several are silent.

1. **`GEOADD` is `lon, lat`.** Opposite of how coordinates are written and spoken. Drivers appear in the wrong hemisphere.
2. **Redis GEO has no per-member TTL.** A TTL on `loc:driver:{id}` does not evict from `loc:drivers:geo`. Without §5.3, dead drivers get offered rides forever.
3. **Geo score vs rating score.** `GEOADD` into a ZSET whose score is a rating destroys the ratings. Two keys, in two services.
4. **Kong doesn't rate-limit WS messages.** It sees the upgrade, then nothing. Cap in-process.
5. **Kong's 60 s `proxy_read_timeout` kills idle WS.** Heartbeat or raise it.
6. **Cross-instance WS push doesn't work by accident.** Pub/Sub from Slice 2, or it works in dev and breaks on the second replica.
7. **`defer ticker.Stop()` inside `Start()`** fires immediately, not when the goroutine exits. This bug already existed in all five services' health checkers. Don't reintroduce it in the sweeper or archiver.
8. **Every-Nth-ping is not a summary.** It deletes turns. §2.5.
9. **`float32` coordinates** carry ~1 m of representational error. Use `float64` everywhere.
10. **Out-of-order pings after reconnect.** Order by `deviceTs`, guard against older writes.
11. **Reverse-geocode cache keys must be rounded** or the hit ratio is zero and you pay per call.
12. **Ride-scoped authorization must expire.** A window that isn't closed on `ride.completed` leaks a passenger's position to a driver indefinitely.
13. **A transaction aborted server-side by Postgres stays aborted** even if the Go code "handles" the error — the billing-service idempotency bug. Return the error from inside `WithinTransaction`, translate to success after it returns.

---

## §17. Open questions — decided 2026-08-12

1. **Stage 1 or Stage 2 from day one?** **Decided: Stage 2 from day one**, overriding `PLAN.md`'s general "new services start at Stage 1" line. §2.8's reasoning holds, and is now cheaper than originally estimated since the Stage-2 boilerplate (decorators, health, shutdown, outbox, consumer loop) lives in `services/common` as thin shims, not per-service code to hand-write.
2. **DynamoDB Local, ScyllaDB, or partitioned Postgres** for raw history? **Deferred to Slice 3** — not needed for Slices 1–2. §6.2's DynamoDB Local recommendation stands as the current plan.
3. **Topic name** for the summary event. **Decided: `ride.summary.ready`.** Deferred to Slice 3 (nothing publishes it yet).
4. **Does `ride-service` adopt `/internal/distance`** for fare estimation? **Deferred to Slice 4.**
5. **Client (passenger) pings — always, or only during an active ride?** **Decided: only during an open tracking window**, per the recommendation. Passenger ingest therefore ships with Slice 2 (it needs the tracking window to exist), not Slice 1.
6. **Does the dashboard (`web/`) get a live map view?** **Deferred** — no observer role added to the WS contract in Slices 1–2. Revisit if/when this is asked for.

**One new decision, not in the original draft:** ingest identity is resolved server-side from Kong's `X-User-Id` via `loc:user:{userId}:driver`, not asserted by the client — see §9's 2026-08-12 correction. No open question here; it removes one.
