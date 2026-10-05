# PLAN.md — Open Work

**What belongs here:** unchecked, currently-open work only. `README.md` describes the target product/architecture; `CLAUDE.md` is current state and terminology for AI sessions; `docs/CHANGELOG.md` is the dated narrative history this file used to carry — bug writeups, verification notes, design rationale. When an item below is finished, delete it (or check it off briefly, then fold the write-up into a new dated `docs/CHANGELOG.md` entry and delete it here) rather than letting it accumulate as a stale checkbox.

**Note on references:** items below name types, functions, and files — never line numbers. A line number is invalidated by the next unrelated edit above it; a symbol survives refactors and is greppable.

---

## Outbox maintenance

- [ ] **No purge/archival job for processed `outbox_message` rows** — these tables only ever grow, in all four schemas (ride, driver, billing, location).

## Test coverage

- [ ] **auth-service has no testcontainers-backed persistence test suite.** Every other Postgres-backed service does (ride, driver, billing, location).
- [ ] **billing-service's `RideSummaryReadyConsumer` has no consumer test**, unlike its `ride.completed`/`ride.cancelled` siblings.
- [ ] **e2e-test only covers location's `POST /batch`** — no coverage of `/ws`, `/rides/{id}/counterparty`, `/rides/{id}/track`, or admin `/positions`.
- [ ] **e2e-test's cancellation coverage only exercises pre-match `Requested` rides.** A scenario asserting 409 when cancelling a driver-started `InProgress` ride is still missing.
- [ ] **The location-radius e2e scenario isn't wired into CI** — it needs a live multi-service `docker-compose` stack, which no CI job in this repo brings up today.

## Dependency modernization

- [ ] `segmentio/kafka-go` (never reached a stable v1) and `lib/pq` (archived upstream, community moved to `jackc/pgx`) are the highest-risk pins repo-wide. Multi-week migration — schedule separately from the smaller items above.

## Codegen (Stage 3, cross-cutting)

- [ ] Generate `contracts/http` from an OpenAPI spec instead of hand-written structs, and/or explore one gRPC call as a learning exercise (e.g. matching-service → driver-service instead of the current HTTP/Kafka-only inter-service calls). Zero codegen exists anywhere in the repo today.

## Observability follow-ups

- [ ] **Two location-service queries still use `if h.metrics != nil`** (`FindNearbyDriversHandler`, `ListLivePositionsHandler`) instead of the noop-client fallback every other handler uses.
- [ ] `services/e2e-test` isn't instrumented with OpenTelemetry — its shared HTTP transport could pick up `otelhttp.NewTransport` to generate realistic demo traces against the observability stack.
- [ ] No sampling strategy beyond `parentbased_always_on` — fine for a low-traffic learning-repo stack, but would need revisiting before any real load.

## Matching algorithm — README's target design, not yet built

- [ ] **TIERED broadcast strategy** (top 2 high-rated drivers first, escalating tiers on timeout) — only the simpler BROADCAST (top 5 at once) is implemented.
- [ ] The ranking formula's third term, `acceptance_rate`, has no data source anywhere in the repo (current ranking is distance+rating only, 0.5/0.5).

## location-service — Slice 4 (of 4)

- [ ] Slice 4 (Geoapify geocoding/routing proxy) is fully unbuilt — `GeocodingProvider`/`RoutingProvider` ports, the Geoapify adapter (timeout/retry/circuit-break, `GEOAPIFY_API_KEY`), `GET /internal/distance`. Slice 3's summary builder already has a `domain.MapMatchingProvider` seam waiting for the same adapter's `MatchRoute` method — until then every summary is `source=Simplified` (RDP + Haversine), never `MapMatched`.

- [ ] `POST /batch` (`LocationHandler`) still calls `Commands.IngestPings` directly instead of going through the `LocationIngestor` port the WS handler uses.
- [ ] Decide on `docs/AUDIT_2026-08-15.md` #9 (the `loc:driver:{id}` 5m TTL vs. staleness sweep) and #10's remaining half (`SpeedMps`/`HeadingDeg` stored unvalidated — needs bounds in LOCATION_SPEC §5.5).
- [ ] `NoopSocketCloser`'s "Stage 1 stand-in" comment (`internal/consumers/socket_closer.go`) is stale — the real WS hub is wired in `cmd/main.go`.
- [ ] Bruno collection has no request for `GET /api/location/rides/{rideId}/counterparty`.

## billing-service — deferred per spec

- [ ] Driver payouts/Connect, client wallet/credits/promos/refunds, pre-ride payment-method validation, FX conversion, receipt rendering, and a reconciliation poller for payments stuck in `processing` (unblocked now that a real async Stripe provider exists, but still not built). See `docs/billing/BILLING_SPEC.md` §9 for the full list and why each stays additive rather than blocking.

## Notification service

- [ ] Fully unbuilt — the last of the README's target services. No directory, schema, or Kafka consumers exist for it.

## Documentation

- [ ] **The 2026-08-18 README/CLAUDE.md audit found ~120 findings; this pass fixed the wrong-as-current claims and the highest-value omissions, not every one.** Lower-priority gaps — mostly components the docs simply never mention rather than describe incorrectly — may remain. If something in README or CLAUDE.md still looks stale, it probably is; re-run the same audit approach (grep the specific claim against the code) rather than assuming the file was fully swept.
