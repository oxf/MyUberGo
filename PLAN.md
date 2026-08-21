# PLAN.md — Open Work

**What belongs here:** unchecked, currently-open work only. `README.md` describes the target product/architecture; `CLAUDE.md` is current state and terminology for AI sessions; `docs/CHANGELOG.md` is the dated narrative history this file used to carry — bug writeups, verification notes, design rationale. When an item below is finished, delete it (or check it off briefly, then fold the write-up into a new dated `docs/CHANGELOG.md` entry and delete it here) rather than letting it accumulate as a stale checkbox.

**Note on references:** items below name types, functions, and files — never line numbers. A line number is invalidated by the next unrelated edit above it; a symbol survives refactors and is greppable.

---

## Outbox durability

- [ ] **The outbox's hot query has no supporting index.** `GetUnprocessedBatch` filters on `processed = false AND (claimed_until IS NULL OR claimed_until < NOW())` ordered by `created_at`, but the only indexes on `outbox_message` in any schema are single-column on the low-cardinality `processed` boolean. A partial index on `(created_at) WHERE processed = false` is the fix. Related and also absent: any purge/archival job for processed rows, so these tables only ever grow.
- [ ] **`services/common/outbox`'s `MarkProcessed`/`IncrementRetries` use the worker's cancellable context**, not a publish-scoped one. On shutdown both can silently fail and the row stays claimed for the full lease duration — a published-but-unmarked message is then republished once the lease expires. A `context.WithoutCancel` or a short bounded context closes it.

## Shared infrastructure

- [ ] **Redis client construction is duplicated verbatim** — including the redisotel wiring and command filter — across matching-service's and location-service's `cmd/main.go`, while Postgres already has a shared `services/common/dbconn` doing DSN resolution, a production-default guard, and env-driven pool tuning. Neither Redis client sets any pool/timeout options at all (bare defaults). Build a `services/common/redisconn` mirroring `dbconn`. Two things whoever does this should know going in: `services/common/go.mod` currently has no go-redis dependency at all, and `REDIS_URL` is misnamed — both services assign it straight to `Addr` as a bare `host:port` and never pass it through `redis.ParseURL`, so adopting `ParseURL` is a behaviour change requiring a `redis://` scheme in every compose env. The matching-concurrency work makes this gap load-bearing rather than cosmetic: a single retry sweep tick can now demand up to `sweepConcurrency × BroadcastSize` (= 8 × 5 = 40, plus overhead) concurrent Redis connections, against go-redis v9's bare default pool (`PoolSize = 10 × GOMAXPROCS`).

## Test coverage

- [ ] **auth-service has no testcontainers-backed persistence test suite.** Every other Postgres-backed service does (ride, driver, billing).
- [ ] **e2e-test's cancellation coverage only exercises pre-match `Requested` rides.** A scenario asserting 409 when cancelling a driver-started `InProgress` ride is still missing.
- [ ] **The location-radius e2e scenario isn't wired into CI** — it needs a live multi-service `docker-compose` stack, which no CI job in this repo brings up today.

## Dependency modernization

- [ ] `segmentio/kafka-go` (never reached a stable v1) and `lib/pq` (archived upstream, community moved to `jackc/pgx`) are the highest-risk pins repo-wide. `x/sys`/`x/crypto` drift across `go.mod` files is also open. Multi-week migration — schedule separately from the smaller items above.

## Codegen (Stage 3, cross-cutting)

- [ ] Generate `contracts/http` from an OpenAPI spec instead of hand-written structs, and/or explore one gRPC call as a learning exercise (e.g. matching-service → driver-service instead of the current HTTP/Kafka-only inter-service calls). Zero codegen exists anywhere in the repo today.

## Observability follow-ups

- [ ] **Three different nil-metrics-guard idioms are scattered across services** (`if h.metrics != nil` in some, a noop-client fallback in others) — worth unifying to one pattern.
- [ ] `services/e2e-test` isn't instrumented with OpenTelemetry — its shared HTTP transport could pick up `otelhttp.NewTransport` to generate realistic demo traces against the observability stack.
- [ ] No sampling strategy beyond `parentbased_always_on` — fine for a low-traffic learning-repo stack, but would need revisiting before any real load.

## Matching algorithm — README's target design, not yet built

- [ ] **TIERED broadcast strategy** (top 2 high-rated drivers first, escalating tiers on timeout) — only the simpler BROADCAST (top 5 at once) is implemented.
- [ ] The ranking formula's third term, `acceptance_rate`, has no data source anywhere in the repo (current ranking is distance+rating only, 0.5/0.5).

## location-service — Slices 2–4 (of 4)

- [ ] **Slice 2 (WebSocket live tracking) is not started.** The `/ws` endpoint, `coder/websocket`, Redis Pub/Sub multi-instance fan-out, and `GET /rides/{rideId}/counterparty` are all open — see `docs/location/LOCATION_SPEC.md`'s own Slice 2 checklist.
- [ ] Slice 3 (Postgres `location` schema, raw ping history, map-matched ride summaries) and Slice 4 (Geoapify geocoding/routing proxy) are both fully unbuilt.

## billing-service — deferred per spec

- [ ] Driver payouts/Connect, client wallet/credits/promos/refunds, pre-ride payment-method validation, FX conversion, receipt rendering, and a reconciliation poller for payments stuck in `processing` (unblocked now that a real async Stripe provider exists, but still not built). See `docs/billing/BILLING_SPEC.md` §9 for the full list and why each stays additive rather than blocking.

## Notification service

- [ ] Fully unbuilt — the last of the README's target services. No directory, schema, or Kafka consumers exist for it.

## Documentation

- [ ] **The 2026-08-18 README/CLAUDE.md audit found ~120 findings; this pass fixed the wrong-as-current claims and the highest-value omissions, not every one.** Lower-priority gaps — mostly components the docs simply never mention rather than describe incorrectly — may remain. If something in README or CLAUDE.md still looks stale, it probably is; re-run the same audit approach (grep the specific claim against the code) rather than assuming the file was fully swept.
