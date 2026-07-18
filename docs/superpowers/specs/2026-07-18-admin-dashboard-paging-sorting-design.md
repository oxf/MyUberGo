# Admin Dashboard (React) + Paging/Sorting on GET List Endpoints

## Context

MyUberGo has no UI — the only ways to inspect data are Bruno requests and psql. The goal is a read-only **dev/admin dashboard** (no auth, agreed with user) with four table screens — Users, Driver Profiles, Shifts, Rides — plus proper server-side paging and sorting on the GET list endpoints that back them.

Decisions made during brainstorming:
- **Paging contract**: offset-based, `?page=N&pageSize=M&sortBy=key&sortDir=asc|desc`; 1-based pages, `offset = (page-1)*pageSize`, defaults `page=1 pageSize=20`, cap 100; response envelope `{items, page, pageSize, totalCount}`.
- **Frontend**: Vite + React + **TypeScript** at repo-root `web/`, hand-rolled reusable `<DataTable>` / `<Pagination>` / `usePagedQuery` (no TanStack). Not in docker-compose — run manually, like `services/e2e-test`. `react-router-dom` for nav; page/sort state lives in URL search params (shareable, survives refresh). **Vite dev proxy** routes `/api/auth|ride|driver` → localhost:8000/8001/8003, so **no CORS changes to any Go service**.

Verified facts driving the plan:
- auth-service has **no list-users endpoint** — `GET /users` is new work (query `auth."user"`, never select `password_hash`). Unused `UserProfileResponse` sits at `services/contracts/http/auth-service.go:42-48`.
- ride-service `GET /ride` (`services/ride-service/cmd/main.go:178`) has a real **off-by-one**: `page` defaults to 1 but `offset := page * pageSize` (line 206) — the first page of results is unreachable by default. Sort is hardcoded `created_at DESC`; response is a bare `[]contracts.RideDto` (and `var rides []...` at line 234 encodes empty as `null`).
- driver-service list handlers (`internal/interfaces/http/handler/{driver,shift}_handler.go` GetList) serialize raw `[]*domain.X` with **no json tags → PascalCase wire keys** (`DriverName`, `LicencePlate`); the camelCase `DriverProfileDto`/`ShiftDto` in `contracts/http/driver-service.go` are unused. Paging is 0-based. Shift timestamps are scanned as raw Postgres text, not RFC3339. `driver.shift` has no `status` column, so `ShiftDto.Status`/`domain.Shift.Status` is always empty (no consumer reads it — verified).
- `services/contracts/go.mod` is `go 1.26` → generic `PagedResponse[T]` is fine.
- Breaking-change consumers: `services/e2e-test` (`apiclient/ride.go:35` decodes a bare array; `actors/client_actor.go:89` calls `ListRides(ctx, 0, 50)`; `ListShifts` exists but is never called) and the Bruno collection (`MyUberGo/Ride Service/Get Ride List.bru` uses `page=0`). CLAUDE.md mandates keeping both current. Several `.bru` files have uncommitted local edits — build on the working tree, don't revert.
- matching-service has no HTTP surface — out of scope.
- `ORDER BY` cannot be a bind parameter → every endpoint needs a **whitelist map** (API sort key → SQL column) validated to 400 on unknown `sortBy`/`sortDir`; `fmt.Sprintf` only ever formats whitelisted values.

Respect the 3-stage architecture: auth/ride stay Stage-1 (all in `cmd/main.go`); driver-service changes follow its existing query/decorator/repository pattern exactly.

## 1. Contracts (`services/contracts/http/`) — first, everything depends on it

- New `common.go`:
  ```go
  type PagedResponse[T any] struct {
      Items      []T `json:"items"`
      Page       int `json:"page"`
      PageSize   int `json:"pageSize"`
      TotalCount int `json:"totalCount"`
  }
  ```
- `auth-service.go`: replace dead `UserProfileResponse` with `UserDto{ID, Email, Name, Phone, Role, CreatedAt}` (camelCase tags).
- `driver-service.go`: drop `ShiftDto.Status` (phantom field, always empty); change `EndedAt` to `*string \`json:"endedAt,omitempty"\`` so open shifts are distinguishable.

## 2. ride-service — `getRideListHandler` (`services/ride-service/cmd/main.go:178-300`)

- Fix off-by-one: validate `page >= 1`, `offset := (page-1) * pageSize`; default pageSize 10 → 20 (keep cap 100).
- Add sort whitelist `{createdAt, status, estimatedPrice, estimatedDistanceKm}` → columns, default `createdAt desc`; small `parseSortParams(r, whitelist, defaultKey)` helper next to existing `parseIntQuery` (`main.go:388`).
- Add `SELECT COUNT(*) FROM ride.ride`; return `contracts.PagedResponse[contracts.RideDto]`; init `rides := []contracts.RideDto{}` so empty encodes as `[]` not `null`.

## 3. auth-service — new `GET /users` (`services/auth-service/cmd/main.go`, Stage-1 idioms)

- Register `mux.HandleFunc("GET /users", getUsersHandler)`; copy the same param-parsing/whitelist helper shape as ride-service.
- Whitelist `{email, name, role, createdAt}`. SQL: `SELECT id, email, name, phone, role, created_at FROM auth."user" ORDER BY <col> <dir> LIMIT $1 OFFSET $2` + `COUNT(*)`. Schema-qualified quoted `auth."user"` (past-bug landmine, see PLAN.md 2026-07-18). **Never select `password_hash`.**
- Respond `PagedResponse[contracts.UserDto]`, `items := []contracts.UserDto{}`.

## 4. driver-service — thread paging/sorting through the Stage-2 layers

- `internal/domain/` (new `paging.go` or in `repository.go`): `PageRequest{Page, PageSize, SortBy, SortDir}` + `DriverProfileSortColumns{createdAt, driverName→name, rating, status, vehicleType, totalRidesCompleted}` and `ShiftSortColumns{startedAt, endedAt, totalRides, totalEarnings}`. Domain owns the whitelists so both handler (validation) and persistence (column mapping) read them without a layering violation.
- `internal/domain/repository.go`: `GetDriverProfileList(ctx, req PageRequest)` / `GetShiftList(ctx, req PageRequest)` + new `CountDriverProfiles(ctx)` / `CountShifts(ctx)`.
- `internal/application/query/`: new `paging.go` with `PagedResult[T any]{Items []T; TotalCount int}`; extend `GetDriverList`/`GetShiftList` structs with `SortBy, SortDir`; handlers call list+count; keep the exact `NewXHandler(...) decorator.QueryHandler[Q,R]` + `ApplyQueryDecorators` construction. Update the two field types in `internal/application/app.go`.
- `internal/persistence/driver_profile_postges_repository.go` / `shift_postgres_repository.go`: `(page-1)*pageSize`, whitelist-mapped `ORDER BY`, add `Count*`. In shift repo, scan `started_at`/`ended_at` via `time.Time`/`sql.NullTime` and format RFC3339 (match how driver-profile `CreatedAt` already does it).
- `internal/interfaces/http/handler/`: `GetList` in both handlers parses page (default 1, min 1) / pageSize (default 20, **add the missing cap 100**) / sort (validate against `domain.XSortColumns` → 400); new `mapping.go` with `toDriverProfileDto` / `toShiftDto` (nil-safe `EndedAt`); respond `PagedResponse[Dto]`. Also switch `GetByID` in both handlers to the same mappers (fixes the PascalCase leak everywhere; e2e decodes case-insensitively so it's safe — update the stale comment at `e2e-test/internal/apiclient/driver.go:11-13`).

## 5. e2e-test simulator (`services/e2e-test/`)

- `apiclient/ride.go` `ListRides` and `apiclient/driver.go` `ListShifts`: decode and **return the envelope** (`PagedResponse[Dto]`) so actors can assert `totalCount`.
- `apiclient/auth.go`: add `ListUsers(ctx, page, pageSize)`.
- `actors/client_actor.go:89`: `ListRides(ctx, 0, 50)` → page 1; adjust `verifyRideInList` for `.Items`. Add `verifyUserInList` (own userID present, `totalCount >= 1`) and a `DriverActor` `verifyShiftInList` using the previously-dead `ListShifts` — keeps the simulator covering all four list endpoints per CLAUDE.md.

## 6. Bruno collection (`MyUberGo/`)

- `Ride Service/Get Ride List.bru`: `page=0` → `1`, add `sortBy`/`sortDir`.
- `Driver Service/Get Driver Profile List.bru`: add paging/sort params.
- New: `Driver Service/Get Driver Shift List.bru`, `Auth Service/Get Users.bru`.

## 7. Frontend — `web/` (greenfield)

Scaffold `npm create vite@latest web -- --template react-ts`; deps: `react-router-dom` only (check `node -v` ≥ 20 for router v7, else pin v6).

```
web/src/
  main.tsx, App.tsx (nav: 4 NavLinks + <Outlet/>), styles.css
  api/types.ts     — PagedResponse<T>, PageParams, UserDto/DriverProfileDto/ShiftDto/RideDto/LocationDto (mirror contracts json tags exactly)
  api/client.ts    — fetchPaged<T>(path, params): Promise<PagedResponse<T>>
  hooks/usePagedQuery.ts — URL-searchParams-backed; returns {data, loading, error, params, setPage, toggleSort}; toggleSort: same col→flip dir, new col→desc+page 1; AbortController cleanup
  components/DataTable.tsx  — Column<T>{key(=API sort key), header, sortable?, render}; props {columns, rows, rowKey, sortBy, sortDir, onSort, loading}
  components/Pagination.tsx — {page, pageSize, totalCount, onPageChange}; numbered pages + prev/next
  pages/{Users,Drivers,Shifts,Rides}Page.tsx — ~40 lines each: usePagedQuery + Column[] + DataTable + Pagination
```

`vite.config.ts` proxy: `/api/auth`→:8000, `/api/ride`→:8001, `/api/driver`→:8003 (rewrite strips the prefix). UI fetches `/api/auth/users`, `/api/ride/ride`, `/api/driver/driver-profile`, `/api/driver/driver-shift`. Sortable columns per screen = exactly the Go whitelists; nullable fields (`endedAt`, `driverId`) render as “—”.

Docs: add a short `web/` section to CLAUDE.md (commands + not-in-compose note, like e2e-test's) and tick progress in PLAN.md.

## Verification

1. `go build ./... ; go vet ./...` in `services/contracts`, `auth-service`, `ride-service`, `driver-service`, `e2e-test`.
2. `docker-compose up --build`, then curl:
   - `localhost:8000/users?page=1&pageSize=5&sortBy=email&sortDir=asc` → envelope, sorted, **no `password_hash`**.
   - `localhost:8001/ride?page=1&pageSize=5` → newest ride present on page 1 (off-by-one fixed); `page=0` → 400; `sortBy=bogus` → 400; empty table → `"items": []`.
   - `localhost:8003/driver-profile?sortBy=rating&sortDir=desc` and `/driver-shift?sortBy=totalEarnings&sortDir=desc` → camelCase keys, no `status` on shifts, RFC3339 timestamps.
   - `totalCount` matches psql `COUNT(*)`.
3. `go run ./cmd` from `services/e2e-test` against the stack — all verifies green including the new list checks.
4. Re-run the four Bruno list requests.
5. `cd web && npm install && npm run dev` — browse all 4 screens with stack + simulator running; click every sortable header both ways, page around, refresh mid-state (URL restores it).

## Commit order

1. contracts: `PagedResponse[T]` + `UserDto`; drop `ShiftDto.Status` / `UserProfileResponse`
2. ride-service: paging fix + sorting + envelope
3. auth-service: `GET /users`
4. driver-service: PageRequest/sort through layers + counts + DTO mapping (List **and** GetByID)
5. e2e-test: envelopes, 1-based pages, users/shifts list coverage
6. bruno: update + new requests
7. web: scaffold + components + 4 pages
8. docs: CLAUDE.md `web/` section, PLAN.md progress note

(2–5 leave stack and simulator briefly inconsistent — do in one sitting; local-only.)

## Risks

- Bare-array → envelope is a deliberate breaking wire change; only known consumers (e2e-test, Bruno) are updated in-plan.
- `fmt.Sprintf` ORDER BY is safe **only** while inputs come from the whitelist maps + validated `asc|desc` — hold that invariant in review.
- `COUNT(*)` and page query run without a shared snapshot — momentary staleness under simulator load is acceptable for an admin view.
- `GET /users` exposes emails/phones unauthenticated on localhost — accepted for this local learning stack (per the no-auth decision).
