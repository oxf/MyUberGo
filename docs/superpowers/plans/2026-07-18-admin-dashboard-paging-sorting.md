# Admin Dashboard + Paged/Sorted List Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only React admin dashboard (`web/`) with Users / Drivers / Shifts / Rides tables, backed by unified server-side paging + sorting on the Go services' GET list endpoints (one of which, `GET /users`, is new).

**Architecture:** A generic `PagedResponse[T]` envelope is added to `services/contracts/http` and adopted by auth-service (new `GET /users`, Stage-1 style), ride-service (`GET /ride`, fixing a paging off-by-one), and driver-service (`GET /driver-profile`, `GET /driver-shift`, threaded through its CQRS layers and normalized to camelCase contracts DTOs). The e2e-test simulator and Bruno collection are updated to the new wire shape. The React app is greenfield Vite + TS with hand-rolled `<DataTable>`/`<Pagination>`/`usePagedQuery`; the Vite dev proxy avoids CORS entirely.

**Tech Stack:** Go 1.26 (per-service modules, `replace ../contracts`), Postgres via lib/pq, Vite + React + TypeScript + react-router-dom v7 (Node 24 confirmed installed). No new Go deps; no frontend table/query libraries.

**Spec:** `docs/superpowers/specs/2026-07-18-admin-dashboard-paging-sorting-design.md`

## Global Constraints

- Paging contract everywhere: `?page=N&pageSize=M&sortBy=key&sortDir=asc|desc`; **1-based** pages; `offset = (page-1)*pageSize`; defaults `page=1`, `pageSize=20`, `sortDir=desc`; `pageSize` cap **100**; violations → HTTP 400.
- Response envelope everywhere: `{"items": [...], "page": N, "pageSize": M, "totalCount": T}`; empty page → `"items": []`, never `null` (initialize slices as `[]T{}`).
- `ORDER BY` is built with `fmt.Sprintf` **only** from a whitelist map (API sort key → SQL column) plus a validated `ASC`/`DESC` literal. Unknown `sortBy`/`sortDir` → 400. Never interpolate raw user input.
- Wire JSON is camelCase (contracts DTOs); timestamps RFC3339; `GET /users` must **never** select or return `password_hash`.
- Respect service stages: auth/ride stay Stage-1 (all in `cmd/main.go`, no layering); driver-service follows its existing query/decorator/repository pattern exactly.
- No CORS headers on any Go service — the browser reaches services only through the Vite dev proxy.
- Go commands run **from the service directory** (each is its own module). Shell is Windows; use Git Bash syntax for `curl` (or `curl.exe` in PowerShell).
- Commit after every task; never commit unrelated working-tree changes (several `MyUberGo/*.bru` files have pre-existing local edits).

---

### Task 1: Contracts — `PagedResponse[T]`, `UserDto`, `ShiftDto` cleanup (+ e2e compile fallout)

**Files:**
- Create: `services/contracts/http/common.go`
- Modify: `services/contracts/http/auth-service.go` (replace `UserProfileResponse`, lines 42-48)
- Modify: `services/contracts/http/driver-service.go` (`ShiftDto`, lines 36-44)
- Modify: `services/e2e-test/internal/actors/driver_actor.go:131,151` (compile fallout of `EndedAt` becoming `*string`)

**Interfaces:**
- Produces: `contracts.PagedResponse[T any]{Items []T; Page, PageSize, TotalCount int}` (json: `items/page/pageSize/totalCount`), `contracts.UserDto{ID, Email, Name, Phone string; Role UserRole; CreatedAt string}` (json camelCase), `ShiftDto` without `Status` and with `EndedAt *string \`json:"endedAt,omitempty"\``. Every later Go task consumes these exact types.

- [ ] **Step 1: Create the envelope**

Create `services/contracts/http/common.go`:

```go
package contracts

// PagedResponse is the shared list-endpoint envelope. Pages are 1-based;
// TotalCount is the unpaged row count so clients can render page numbers.
type PagedResponse[T any] struct {
	Items      []T `json:"items"`
	Page       int `json:"page"`
	PageSize   int `json:"pageSize"`
	TotalCount int `json:"totalCount"`
}
```

- [ ] **Step 2: Replace `UserProfileResponse` with `UserDto`**

In `services/contracts/http/auth-service.go`, replace lines 42-48 (`UserProfileResponse` — verified unused anywhere) with:

```go
type UserDto struct {
	ID        string   `json:"id"`
	Email     string   `json:"email"`
	Name      string   `json:"name"`
	Phone     string   `json:"phone"`
	Role      UserRole `json:"role"`
	CreatedAt string   `json:"createdAt"`
}
```

- [ ] **Step 3: Clean up `ShiftDto`**

In `services/contracts/http/driver-service.go`, replace the `ShiftDto` block (lines 36-44) with (drops phantom `Status` — `driver.shift` has no status column and no consumer reads it; makes `EndedAt` nullable):

```go
type ShiftDto struct {
	Id            string  `json:"id"`
	DriverId      string  `json:"driverId"`
	StartedAt     string  `json:"startedAt"`
	EndedAt       *string `json:"endedAt,omitempty"`
	TotalRides    int     `json:"totalRides"`
	TotalEarnings float64 `json:"totalEarnings"`
}
```

- [ ] **Step 4: Fix e2e-test compile fallout**

`services/e2e-test/internal/actors/driver_actor.go` compares `shift.EndedAt` as a string in two places. In `verifyOpenShift` (line 131) replace

```go
		v.Eq("endedAt", shift.EndedAt, "")
```

with

```go
		v.True("endedAt", shift.EndedAt == nil, "expected open shift (endedAt null)")
```

In `verifyEndedShift` (line 151) replace

```go
		v.NotEmpty("endedAt", shift.EndedAt)
```

with

```go
		v.True("endedAt", shift.EndedAt != nil && *shift.EndedAt != "", "expected endedAt to be set")
```

- [ ] **Step 5: Verify everything still compiles**

```bash
cd services/contracts && go build ./... && go vet ./...
cd ../e2e-test && go build ./... && go vet ./...
cd ../driver-service && go build ./...
cd ../auth-service && go build ./...
cd ../ride-service && go build ./...
```
Expected: all succeed (driver-service never referenced `ShiftDto.Status`; `UserProfileResponse` was dead).

- [ ] **Step 6: Commit**

```bash
git add services/contracts/http services/e2e-test/internal/actors/driver_actor.go
git commit -m "contracts: add PagedResponse[T] and UserDto; drop dead ShiftDto.Status; nullable endedAt"
```

---

### Task 2: ride-service — paging fix, sorting, envelope on `GET /ride`

**Files:**
- Modify: `services/ride-service/cmd/main.go` (imports line 3-19, `getRideListHandler` lines 178-300, new helpers next to `parseIntQuery` line 388)
- Test: `services/ride-service/cmd/main_test.go` (new)

**Interfaces:**
- Consumes: `contracts.PagedResponse[contracts.RideDto]` from Task 1.
- Produces: `GET /ride` returns the envelope; sort keys `createdAt|status|estimatedPrice|estimatedDistanceKm`. Internal helper `parseListParams(r *http.Request, sortColumns map[string]string, defaultSortKey string) (listParams, error)` where `listParams{page, pageSize int; sortCol, sortDir string}` (`sortCol` is the **SQL column**, `sortDir` is `"ASC"|"DESC"`). Task 3 duplicates this helper in auth-service (Stage-1 services deliberately don't share code).

- [ ] **Step 1: Write the failing test**

Create `services/ride-service/cmd/main_test.go`:

```go
package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/ride", nil)
	p, err := parseListParams(r, rideSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortCol != "created_at" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/ride?page=0",
		"/ride?page=-1",
		"/ride?pageSize=0",
		"/ride?pageSize=101",
		"/ride?sortBy=lol",
		"/ride?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, rideSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ExplicitSortAndPaging(t *testing.T) {
	r := httptest.NewRequest("GET", "/ride?page=3&pageSize=50&sortBy=estimatedPrice&sortDir=asc", nil)
	p, err := parseListParams(r, rideSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 3 || p.pageSize != 50 || p.sortCol != "estimated_price" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/ride-service`): `go test ./cmd -run TestParseListParams -v`
Expected: FAIL to compile — `undefined: parseListParams`, `undefined: rideSortColumns`.

- [ ] **Step 3: Implement the helper and whitelist**

In `services/ride-service/cmd/main.go`: add `"strings"` to the import block (after `"strconv"`). Then add below `parseIntQuery` (after line 405):

```go
var rideSortColumns = map[string]string{
	"createdAt":           "created_at",
	"status":              "status",
	"estimatedPrice":      "estimated_price",
	"estimatedDistanceKm": "estimated_distance_km",
}

// listParams is a validated set of paging/sorting query params. sortCol is a
// SQL column straight from a whitelist map and sortDir is "ASC"/"DESC" — the
// only values ever interpolated into ORDER BY.
type listParams struct {
	page     int
	pageSize int
	sortCol  string
	sortDir  string
}

func parseListParams(r *http.Request, sortColumns map[string]string, defaultSortKey string) (listParams, error) {
	page, err := parseIntQuery(r, "page", 1)
	if err != nil {
		return listParams{}, err
	}
	if page < 1 {
		return listParams{}, fmt.Errorf("page must be >= 1")
	}

	pageSize, err := parseIntQuery(r, "pageSize", 20)
	if err != nil {
		return listParams{}, err
	}
	if pageSize < 1 {
		return listParams{}, fmt.Errorf("pageSize must be >= 1")
	}
	if pageSize > 100 {
		return listParams{}, fmt.Errorf("pageSize cannot exceed 100")
	}

	sortKey := r.URL.Query().Get("sortBy")
	if sortKey == "" {
		sortKey = defaultSortKey
	}
	sortCol, ok := sortColumns[sortKey]
	if !ok {
		return listParams{}, fmt.Errorf("unknown sortBy %q", sortKey)
	}

	sortDir := strings.ToLower(r.URL.Query().Get("sortDir"))
	switch sortDir {
	case "":
		sortDir = "desc"
	case "asc", "desc":
	default:
		return listParams{}, fmt.Errorf("sortDir must be asc or desc")
	}

	return listParams{page: page, pageSize: pageSize, sortCol: sortCol, sortDir: strings.ToUpper(sortDir)}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd -run TestParseListParams -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Rewrite the handler**

Replace the top of `getRideListHandler` (lines 178-234, everything from the function signature through `var rides []contracts.RideDto`) with:

```go
func getRideListHandler(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, rideSortColumns, "createdAt")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ride.ride`).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	query := fmt.Sprintf(`
		SELECT
			id,
			client_id,
			driver_id,
			status,
			pickup_lat,
			pickup_lng,
			pickup_address,
			dest_lat,
			dest_lng,
			dest_address,
			estimated_price,
			estimated_distance_km,
			created_at
		FROM ride.ride
		ORDER BY %s %s
		LIMIT $1 OFFSET $2
	`, params.sortCol, params.sortDir)

	rows, err := db.Query(query, params.pageSize, (params.page-1)*params.pageSize)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	rides := []contracts.RideDto{}
```

Keep the existing scan loop (lines 236-292) unchanged. Then replace the final encode (lines 294-300) with:

```go
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(contracts.PagedResponse[contracts.RideDto]{
		Items:      rides,
		Page:       params.page,
		PageSize:   params.pageSize,
		TotalCount: total,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
```

- [ ] **Step 6: Build, vet, test**

Run (from `services/ride-service`): `go build ./... && go vet ./... && go test ./cmd -v`
Expected: build/vet clean, tests PASS. (The e2e simulator's `ListRides` is now runtime-incompatible until Task 6 — expected mid-plan state; it still compiles.)

- [ ] **Step 7: Commit**

```bash
git add services/ride-service/cmd
git commit -m "ride-service: 1-based paging (fixes skipped first page), sort whitelist, totalCount envelope on GET /ride"
```

---

### Task 3: auth-service — new `GET /users`

**Files:**
- Modify: `services/auth-service/cmd/main.go` (imports lines 3-17, route registration line 46, new handler + helpers at end of file)
- Test: `services/auth-service/cmd/main_test.go` (new)

**Interfaces:**
- Consumes: `contracts.PagedResponse[contracts.UserDto]`, `contracts.UserDto` from Task 1.
- Produces: `GET /users` on :8000 returning the envelope; sort keys `email|name|role|createdAt`. Helpers `parseIntQuery` and `parseListParams`/`listParams` (deliberate Stage-1 duplicates of ride-service's — same signatures as Task 2).

- [ ] **Step 1: Write the failing test**

Create `services/auth-service/cmd/main_test.go`:

```go
package main

import (
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/users", nil)
	p, err := parseListParams(r, userSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortCol != "created_at" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/users?page=0",
		"/users?pageSize=101",
		"/users?sortBy=password_hash", // must not be sortable/selectable
		"/users?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, userSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ExplicitSort(t *testing.T) {
	r := httptest.NewRequest("GET", "/users?sortBy=email&sortDir=asc", nil)
	p, err := parseListParams(r, userSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.sortCol != "email" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (from `services/auth-service`): `go test ./cmd -run TestParseListParams -v`
Expected: FAIL to compile — `undefined: parseListParams`, `undefined: userSortColumns`.

- [ ] **Step 3: Implement helpers, whitelist, handler, route**

In `services/auth-service/cmd/main.go`:

(a) Add `"fmt"` and `"strings"` to the import block.

(b) Register the route after line 46 (`mux.HandleFunc("POST /refresh", refreshHandler)`):

```go
	mux.HandleFunc("GET /users", getUsersHandler)
```

(c) Append at the end of the file (helpers are Stage-1 duplicates of ride-service's, same shape):

```go
var userSortColumns = map[string]string{
	"email":     "email",
	"name":      "name",
	"role":      "role",
	"createdAt": "created_at",
}

// listParams is a validated set of paging/sorting query params. sortCol is a
// SQL column straight from a whitelist map and sortDir is "ASC"/"DESC" — the
// only values ever interpolated into ORDER BY.
type listParams struct {
	page     int
	pageSize int
	sortCol  string
	sortDir  string
}

func parseIntQuery(r *http.Request, key string, defaultValue int) (int, error) {
	valStr := r.URL.Query().Get(key)

	if valStr == "" {
		return defaultValue, nil
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		return 0, err
	}

	if val < 0 {
		return 0, fmt.Errorf("%s cannot be negative", key)
	}

	return val, nil
}

func parseListParams(r *http.Request, sortColumns map[string]string, defaultSortKey string) (listParams, error) {
	page, err := parseIntQuery(r, "page", 1)
	if err != nil {
		return listParams{}, err
	}
	if page < 1 {
		return listParams{}, fmt.Errorf("page must be >= 1")
	}

	pageSize, err := parseIntQuery(r, "pageSize", 20)
	if err != nil {
		return listParams{}, err
	}
	if pageSize < 1 {
		return listParams{}, fmt.Errorf("pageSize must be >= 1")
	}
	if pageSize > 100 {
		return listParams{}, fmt.Errorf("pageSize cannot exceed 100")
	}

	sortKey := r.URL.Query().Get("sortBy")
	if sortKey == "" {
		sortKey = defaultSortKey
	}
	sortCol, ok := sortColumns[sortKey]
	if !ok {
		return listParams{}, fmt.Errorf("unknown sortBy %q", sortKey)
	}

	sortDir := strings.ToLower(r.URL.Query().Get("sortDir"))
	switch sortDir {
	case "":
		sortDir = "desc"
	case "asc", "desc":
	default:
		return listParams{}, fmt.Errorf("sortDir must be asc or desc")
	}

	return listParams{page: page, pageSize: pageSize, sortCol: sortCol, sortDir: strings.ToUpper(sortDir)}, nil
}

func getUsersHandler(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, userSortColumns, "createdAt")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM auth."user"`).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// password_hash is deliberately never selected here.
	query := fmt.Sprintf(`
		SELECT id, email, name, phone, role, created_at
		FROM auth."user"
		ORDER BY %s %s
		LIMIT $1 OFFSET $2
	`, params.sortCol, params.sortDir)

	rows, err := db.Query(query, params.pageSize, (params.page-1)*params.pageSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	users := []contracts.UserDto{}
	for rows.Next() {
		var u contracts.UserDto
		var createdAt time.Time
		if err := rows.Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.Role, &createdAt); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		u.CreatedAt = createdAt.Format(time.RFC3339)
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(contracts.PagedResponse[contracts.UserDto]{
		Items:      users,
		Page:       params.page,
		PageSize:   params.pageSize,
		TotalCount: total,
	})
}
```

Note: `auth."user"` must stay schema-qualified **and quoted** — `user` is a Postgres reserved word, and unqualified table names were a real past bug here (PLAN.md 2026-07-18).

- [ ] **Step 4: Run test to verify it passes; build and vet**

Run (from `services/auth-service`): `go test ./cmd -v && go build ./... && go vet ./...`
Expected: 3 tests PASS, build/vet clean.

- [ ] **Step 5: Commit**

```bash
git add services/auth-service/cmd
git commit -m "auth-service: add GET /users with paging, sort whitelist, and PagedResponse envelope"
```

---

### Task 4: driver-service — paging/sorting through domain, application, persistence

**Files:**
- Create: `services/driver-service/internal/domain/paging.go`
- Create: `services/driver-service/internal/application/query/paging.go`
- Modify: `services/driver-service/internal/domain/repository.go`
- Modify: `services/driver-service/internal/application/query/get_driver_list.go`
- Modify: `services/driver-service/internal/application/query/get_shift_list.go`
- Modify: `services/driver-service/internal/application/app.go` (lines 23, 25)
- Modify: `services/driver-service/internal/persistence/driver_profile_postges_repository.go` (GetDriverProfileList, lines 52-77)
- Modify: `services/driver-service/internal/persistence/shift_postgres_repository.go` (GetShiftList lines 88-115, GetShiftByID lines 117-137)

**Interfaces:**
- Consumes: nothing new from earlier tasks (pure internal threading).
- Produces (Task 5 relies on these exact names): `domain.PageRequest{Page, PageSize int; SortBy, SortDir string}`; `domain.DriverProfileSortColumns` / `domain.ShiftSortColumns` (`map[string]string`, API key → SQL column); repo methods `GetDriverProfileList(ctx, domain.PageRequest)`, `CountDriverProfiles(ctx) (int, error)`, `GetShiftList(ctx, domain.PageRequest)`, `CountShifts(ctx) (int, error)`; `query.PagedResult[T any]{Items []T; TotalCount int}`; query structs `GetDriverList` / `GetShiftList` gain `SortBy, SortDir string`; `app.Queries.GetDriverList` is `decorator.QueryHandler[query.GetDriverList, query.PagedResult[*domain.DriverProfile]]` (shift analogous).

- [ ] **Step 1: Add domain paging types**

Create `services/driver-service/internal/domain/paging.go`:

```go
package domain

// PageRequest carries validated list-query paging/sorting. Page is 1-based.
// SortBy is a key of the entity's sort-column map and SortDir is "ASC" or
// "DESC" — the HTTP layer validates both before building a PageRequest.
type PageRequest struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

// Sort whitelists: API sort key -> SQL column. ORDER BY cannot be
// parameterized, so persistence interpolates only values from these maps.
var DriverProfileSortColumns = map[string]string{
	"createdAt":           "created_at",
	"driverName":          "name",
	"rating":              "rating",
	"status":              "status",
	"vehicleType":         "vehicle_type",
	"totalRidesCompleted": "total_rides_completed",
}

var ShiftSortColumns = map[string]string{
	"startedAt":     "started_at",
	"endedAt":       "ended_at",
	"totalRides":    "total_rides",
	"totalEarnings": "total_earnings",
}
```

- [ ] **Step 2: Update repository interfaces**

Replace `services/driver-service/internal/domain/repository.go` contents:

```go
package domain

import "context"

type DriverProfileRepository interface {
	CreateDriverProfile(ctx context.Context, profile *DriverProfile) (string, error)
	UpdateDriverProfile(ctx context.Context, id, name, phone, vehicleType, licencePlate string) error
	GetDriverProfileList(ctx context.Context, req PageRequest) ([]*DriverProfile, error)
	CountDriverProfiles(ctx context.Context) (int, error)
	GetDriverProfileByID(ctx context.Context, id string) (*DriverProfile, error)
}

type ShiftRepository interface {
	CreateShift(ctx context.Context, shift *Shift) (string, error)
	UpdateShift(ctx context.Context, shift *Shift) error
	HasActiveShift(ctx context.Context, driverID string) (bool, error)
	EndShift(ctx context.Context, id string) error
	GetShiftList(ctx context.Context, req PageRequest) ([]*Shift, error)
	CountShifts(ctx context.Context) (int, error)
	GetShiftByID(ctx context.Context, id string) (*Shift, error)
}
```

- [ ] **Step 3: Add the application-layer paged result**

Create `services/driver-service/internal/application/query/paging.go`:

```go
package query

// PagedResult pairs one page of items with the unpaged total, so the HTTP
// layer can build the wire envelope without a second query round-trip.
type PagedResult[T any] struct {
	Items      []T
	TotalCount int
}
```

- [ ] **Step 4: Update the two list query handlers**

Replace `services/driver-service/internal/application/query/get_driver_list.go` contents:

```go
package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetDriverList struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type GetDriverListHandler struct {
	repo domain.DriverProfileRepository
}

func NewGetDriverListHandler(repo domain.DriverProfileRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetDriverList, PagedResult[*domain.DriverProfile]] {
	handler := &GetDriverListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetDriverList, PagedResult[*domain.DriverProfile]](handler, logger, metricsClient)
}

func (h *GetDriverListHandler) Handle(ctx context.Context, q GetDriverList) (PagedResult[*domain.DriverProfile], error) {
	total, err := h.repo.CountDriverProfiles(ctx)
	if err != nil {
		return PagedResult[*domain.DriverProfile]{}, err
	}

	items, err := h.repo.GetDriverProfileList(ctx, domain.PageRequest{
		Page: q.Page, PageSize: q.PageSize, SortBy: q.SortBy, SortDir: q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.DriverProfile]{}, err
	}

	return PagedResult[*domain.DriverProfile]{Items: items, TotalCount: total}, nil
}
```

Replace `services/driver-service/internal/application/query/get_shift_list.go` contents:

```go
package query

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetShiftList struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type GetShiftListHandler struct {
	repo domain.ShiftRepository
}

func NewGetShiftListHandler(repo domain.ShiftRepository, logger *logrus.Entry, metricsClient decorator.MetricsClient) decorator.QueryHandler[GetShiftList, PagedResult[*domain.Shift]] {
	handler := &GetShiftListHandler{repo: repo}
	return decorator.ApplyQueryDecorators[GetShiftList, PagedResult[*domain.Shift]](handler, logger, metricsClient)
}

func (h *GetShiftListHandler) Handle(ctx context.Context, q GetShiftList) (PagedResult[*domain.Shift], error) {
	total, err := h.repo.CountShifts(ctx)
	if err != nil {
		return PagedResult[*domain.Shift]{}, err
	}

	items, err := h.repo.GetShiftList(ctx, domain.PageRequest{
		Page: q.Page, PageSize: q.PageSize, SortBy: q.SortBy, SortDir: q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.Shift]{}, err
	}

	return PagedResult[*domain.Shift]{Items: items, TotalCount: total}, nil
}
```

- [ ] **Step 5: Update `app.go` query field types**

In `services/driver-service/internal/application/app.go`, replace lines 23 and 25:

```go
	GetDriverList decorator.QueryHandler[query.GetDriverList, query.PagedResult[*domain.DriverProfile]]
```
```go
	GetShiftList  decorator.QueryHandler[query.GetShiftList, query.PagedResult[*domain.Shift]]
```

(`GetDriverByID`/`GetShiftByID` stay unchanged. `cmd/main.go` needs no edits — constructor calls are identical.)

- [ ] **Step 6: Update the driver-profile repository**

In `services/driver-service/internal/persistence/driver_profile_postges_repository.go`: add `"fmt"` to imports, then replace `GetDriverProfileList` (lines 52-77) with:

```go
func (r *PostgresDriverProfileRepository) GetDriverProfileList(ctx context.Context, req domain.PageRequest) ([]*domain.DriverProfile, error) {
	col, ok := domain.DriverProfileSortColumns[req.SortBy]
	if !ok {
		col = "created_at" // handler validates; this is a defensive fallback
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(`
        SELECT id, user_id, name, phone, rating, vehicle_type, license_plate, status, total_rides_completed, created_at
        FROM driver.driver_profile
        ORDER BY %s %s
        LIMIT $1 OFFSET $2
    `, col, dir)

	rows, err := r.db.QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.DriverProfile
	for rows.Next() {
		var d domain.DriverProfile
		var createdAt time.Time
		if err := rows.Scan(&d.ID, &d.UserID, &d.DriverName, &d.Phone, &d.Rating,
			&d.VehicleType, &d.LicencePlate, &d.Status, &d.TotalRidesCompleted, &createdAt); err != nil {
			return nil, err
		}
		d.CreatedAt = createdAt.Format(time.RFC3339)
		result = append(result, &d)
	}
	return result, rows.Err()
}

func (r *PostgresDriverProfileRepository) CountDriverProfiles(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM driver.driver_profile`).Scan(&n)
	return n, err
}
```

- [ ] **Step 7: Update the shift repository (sorting + counts + RFC3339 timestamps)**

In `services/driver-service/internal/persistence/shift_postgres_repository.go`: add `"fmt"` and `"time"` to imports, then replace `GetShiftList` (lines 88-115) and `GetShiftByID` (lines 117-137) with:

```go
func (r *PostgresShiftRepository) GetShiftList(ctx context.Context, req domain.PageRequest) ([]*domain.Shift, error) {
	col, ok := domain.ShiftSortColumns[req.SortBy]
	if !ok {
		col = "started_at" // handler validates; this is a defensive fallback
	}
	dir := req.SortDir
	if dir != "ASC" && dir != "DESC" {
		dir = "DESC"
	}

	query := fmt.Sprintf(`
        SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
        FROM driver.shift
        ORDER BY %s %s
        LIMIT $1 OFFSET $2
    `, col, dir)

	rows, err := r.db.QueryContext(ctx, query, req.PageSize, (req.Page-1)*req.PageSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*domain.Shift
	for rows.Next() {
		s, err := scanShift(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

func (r *PostgresShiftRepository) CountShifts(ctx context.Context) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM driver.shift`).Scan(&n)
	return n, err
}

func (r *PostgresShiftRepository) GetShiftByID(ctx context.Context, id string) (*domain.Shift, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, driver_id, started_at, ended_at, total_rides, total_earnings
		FROM driver.shift
		WHERE id = $1
	`, id)

	s, err := scanShift(row)
	if err == sql.ErrNoRows {
		return nil, commonerrors.ErrNotFound
	}
	return s, err
}

// scanShift reads one shift row, normalizing timestamps to RFC3339 (matching
// driver-profile CreatedAt) instead of the driver's raw time encoding.
func scanShift(row interface{ Scan(dest ...any) error }) (*domain.Shift, error) {
	var d domain.Shift
	var startedAt time.Time
	var endedAt sql.NullTime

	if err := row.Scan(&d.ID, &d.DriverID, &startedAt, &endedAt, &d.TotalRides, &d.TotalEarnings); err != nil {
		return nil, err
	}

	d.StartedAt = startedAt.Format(time.RFC3339)
	if endedAt.Valid {
		s := endedAt.Time.Format(time.RFC3339)
		d.EndedAt = &s
	}
	return &d, nil
}
```

- [ ] **Step 8: Build and vet**

Run (from `services/driver-service`): `go build ./... && go vet ./...`
Expected: **clean**. The handlers compile unchanged (the query structs only gained fields, and `writeJSON` takes `any`) — but `GetList` now serializes the internal `PagedResult` with PascalCase keys. That's a known-broken intermediate wire shape; Task 5 replaces those handlers.

- [ ] **Step 9: Commit**

```bash
git add services/driver-service/internal
git commit -m "driver-service: thread PageRequest + sort whitelists through domain/query/persistence; add counts; RFC3339 shift timestamps"
```

---

### Task 5: driver-service — HTTP handlers: validation, DTO mapping, envelope

**Files:**
- Create: `services/driver-service/internal/interfaces/http/handler/paging.go`
- Create: `services/driver-service/internal/interfaces/http/handler/mapping.go`
- Modify: `services/driver-service/internal/interfaces/http/handler/driver_handler.go` (`GetList` lines 65-75, `GetByID` lines 77-85, `parseIntQuery` lines 96-113)
- Modify: `services/driver-service/internal/interfaces/http/handler/shift_handler.go` (`GetList` lines 61-71, `GetByID` lines 73-81)
- Test: `services/driver-service/internal/interfaces/http/handler/paging_test.go`, `.../mapping_test.go` (new)

**Interfaces:**
- Consumes: `domain.PageRequest`, `domain.DriverProfileSortColumns`, `domain.ShiftSortColumns`, `query.PagedResult`, updated `query.GetDriverList`/`GetShiftList` (Task 4); `contracts.PagedResponse`, updated `ShiftDto` (Task 1).
- Produces: `GET /driver-profile` and `GET /driver-shift` return `PagedResponse[DriverProfileDto]` / `PagedResponse[ShiftDto]` (camelCase); `GET /driver-profile/{id}` / `GET /driver-shift/{id}` return the DTOs too (fixes the PascalCase leak). Internal: `listParams{page, pageSize int; sortBy, sortDir string}` — note `sortBy` here is the **API key** (persistence maps it), unlike Stage-1 services; `sortDir` is `"ASC"|"DESC"`. `toDriverProfileDto(*domain.DriverProfile) contracts.DriverProfileDto`, `toShiftDto(*domain.Shift) contracts.ShiftDto`.

- [ ] **Step 1: Write the failing tests**

Create `services/driver-service/internal/interfaces/http/handler/paging_test.go`:

```go
package handler

import (
	"driver-service/internal/domain"
	"net/http/httptest"
	"testing"
)

func TestParseListParams_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/driver-profile", nil)
	p, err := parseListParams(r, domain.DriverProfileSortColumns, "createdAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.page != 1 || p.pageSize != 20 || p.sortBy != "createdAt" || p.sortDir != "DESC" {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestParseListParams_RejectsBadInput(t *testing.T) {
	for _, url := range []string{
		"/driver-profile?page=0",
		"/driver-profile?pageSize=0",
		"/driver-profile?pageSize=101",
		"/driver-profile?sortBy=name", // SQL column, not API key — must 400
		"/driver-profile?sortDir=sideways",
	} {
		r := httptest.NewRequest("GET", url, nil)
		if _, err := parseListParams(r, domain.DriverProfileSortColumns, "createdAt"); err == nil {
			t.Errorf("expected error for %s", url)
		}
	}
}

func TestParseListParams_ShiftSortKeys(t *testing.T) {
	r := httptest.NewRequest("GET", "/driver-shift?sortBy=totalEarnings&sortDir=asc", nil)
	p, err := parseListParams(r, domain.ShiftSortColumns, "startedAt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.sortBy != "totalEarnings" || p.sortDir != "ASC" {
		t.Fatalf("unexpected params: %+v", p)
	}
}
```

Create `services/driver-service/internal/interfaces/http/handler/mapping_test.go`:

```go
package handler

import (
	"driver-service/internal/domain"
	"testing"
)

func TestToDriverProfileDto(t *testing.T) {
	dto := toDriverProfileDto(&domain.DriverProfile{
		ID: "p1", UserID: "u1", DriverName: "Ann", Phone: "+357501234567",
		Rating: 4.9, VehicleType: "Sedan", LicencePlate: "AA1234BB",
		Status: "Online", TotalRidesCompleted: 7, CreatedAt: "2026-07-18T10:00:00Z",
	})
	if dto.Id != "p1" || dto.UserId != "u1" || dto.DriverName != "Ann" || dto.TotalRidesCompleted != 7 {
		t.Fatalf("bad mapping: %+v", dto)
	}
}

func TestToShiftDto_OpenAndEnded(t *testing.T) {
	open := toShiftDto(&domain.Shift{ID: "s1", DriverID: "p1", StartedAt: "2026-07-18T10:00:00Z"})
	if open.EndedAt != nil {
		t.Fatalf("expected nil EndedAt for open shift, got %v", *open.EndedAt)
	}

	ended := "2026-07-18T12:00:00Z"
	done := toShiftDto(&domain.Shift{ID: "s2", DriverID: "p1", StartedAt: "2026-07-18T10:00:00Z", EndedAt: &ended})
	if done.EndedAt == nil || *done.EndedAt != ended {
		t.Fatalf("expected EndedAt %q, got %v", ended, done.EndedAt)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run (from `services/driver-service`): `go test ./internal/interfaces/http/handler -v`
Expected: FAIL to compile — `undefined: parseListParams`, `undefined: toDriverProfileDto`, `undefined: toShiftDto` (`parseIntQuery` already exists in `driver_handler.go` and is reused as-is).

- [ ] **Step 3: Implement `paging.go` and `mapping.go`**

Create `services/driver-service/internal/interfaces/http/handler/paging.go`:

```go
package handler

import (
	"fmt"
	"net/http"
	"strings"
)

// listParams is a validated set of paging/sorting query params. sortBy is an
// API sort key already checked against the entity's domain sort-column map
// (persistence does the column mapping); sortDir is "ASC" or "DESC".
type listParams struct {
	page     int
	pageSize int
	sortBy   string
	sortDir  string
}

func parseListParams(r *http.Request, sortColumns map[string]string, defaultSortKey string) (listParams, error) {
	page, err := parseIntQuery(r, "page", 1)
	if err != nil {
		return listParams{}, err
	}
	if page < 1 {
		return listParams{}, fmt.Errorf("page must be >= 1")
	}

	pageSize, err := parseIntQuery(r, "pageSize", 20)
	if err != nil {
		return listParams{}, err
	}
	if pageSize < 1 {
		return listParams{}, fmt.Errorf("pageSize must be >= 1")
	}
	if pageSize > 100 {
		return listParams{}, fmt.Errorf("pageSize cannot exceed 100")
	}

	sortBy := r.URL.Query().Get("sortBy")
	if sortBy == "" {
		sortBy = defaultSortKey
	}
	if _, ok := sortColumns[sortBy]; !ok {
		return listParams{}, fmt.Errorf("unknown sortBy %q", sortBy)
	}

	sortDir := strings.ToLower(r.URL.Query().Get("sortDir"))
	switch sortDir {
	case "":
		sortDir = "desc"
	case "asc", "desc":
	default:
		return listParams{}, fmt.Errorf("sortDir must be asc or desc")
	}

	return listParams{page: page, pageSize: pageSize, sortBy: sortBy, sortDir: strings.ToUpper(sortDir)}, nil
}
```

Create `services/driver-service/internal/interfaces/http/handler/mapping.go`:

```go
package handler

import (
	"driver-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
)

func toDriverProfileDto(d *domain.DriverProfile) contracts.DriverProfileDto {
	return contracts.DriverProfileDto{
		Id:                  d.ID,
		UserId:              d.UserID,
		DriverName:          d.DriverName,
		Phone:               d.Phone,
		Rating:              d.Rating,
		VehicleType:         d.VehicleType,
		LicencePlate:        d.LicencePlate,
		Status:              d.Status,
		TotalRidesCompleted: d.TotalRidesCompleted,
		CreatedAt:           d.CreatedAt,
	}
}

func toShiftDto(s *domain.Shift) contracts.ShiftDto {
	return contracts.ShiftDto{
		Id:            s.ID,
		DriverId:      s.DriverID,
		StartedAt:     s.StartedAt,
		EndedAt:       s.EndedAt,
		TotalRides:    s.TotalRides,
		TotalEarnings: s.TotalEarnings,
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/interfaces/http/handler -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Rewrite the four GET handlers**

In `services/driver-service/internal/interfaces/http/handler/driver_handler.go`: add `"driver-service/internal/domain"` to imports. Replace `GetList` (lines 65-75) and `GetByID` (lines 77-85) with:

```go
func (h *DriverProfileHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, domain.DriverProfileSortColumns, "createdAt")
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetDriverList.Handle(r.Context(), query.GetDriverList{
		Page: params.page, PageSize: params.pageSize, SortBy: params.sortBy, SortDir: params.sortDir,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	items := make([]contracts.DriverProfileDto, 0, len(result.Items))
	for _, d := range result.Items {
		items = append(items, toDriverProfileDto(d))
	}
	writeJSON(w, contracts.PagedResponse[contracts.DriverProfileDto]{
		Items: items, Page: params.page, PageSize: params.pageSize, TotalCount: result.TotalCount,
	})
}

func (h *DriverProfileHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetDriverByID.Handle(r.Context(), query.GetDriverByID{ID: id})
	if err != nil || result == nil {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, toDriverProfileDto(result))
}
```

In `services/driver-service/internal/interfaces/http/handler/shift_handler.go`: add `"driver-service/internal/domain"` and `contracts "github.com/oxf/MyUber/contracts/http"` imports are already present (contracts is; add domain). Replace `GetList` (lines 61-71) and `GetByID` (lines 73-81) with:

```go
func (h *ShiftHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, domain.ShiftSortColumns, "startedAt")
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetShiftList.Handle(r.Context(), query.GetShiftList{
		Page: params.page, PageSize: params.pageSize, SortBy: params.sortBy, SortDir: params.sortDir,
	})
	if err != nil {
		writeError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	items := make([]contracts.ShiftDto, 0, len(result.Items))
	for _, s := range result.Items {
		items = append(items, toShiftDto(s))
	}
	writeJSON(w, contracts.PagedResponse[contracts.ShiftDto]{
		Items: items, Page: params.page, PageSize: params.pageSize, TotalCount: result.TotalCount,
	})
}

func (h *ShiftHandler) GetByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, err := h.app.Queries.GetShiftByID.Handle(r.Context(), query.GetShiftByID{ID: id})
	if err != nil || result == nil {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, toShiftDto(result))
}
```

- [ ] **Step 6: Build, vet, full service test run**

Run (from `services/driver-service`): `go build ./... && go vet ./... && go test ./...`
Expected: clean build/vet, all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add services/driver-service/internal
git commit -m "driver-service: paged/sorted GetList with camelCase DTO envelope; GetByID DTO mapping (fixes PascalCase leak)"
```

---

### Task 6: e2e-test simulator — adopt envelopes, cover `/users` and `/driver-shift`

**Files:**
- Modify: `services/e2e-test/internal/apiclient/ride.go` (`ListRides`, lines 35-40)
- Modify: `services/e2e-test/internal/apiclient/driver.go` (comment lines 11-14, `ListShifts` lines 55-60)
- Modify: `services/e2e-test/internal/apiclient/auth.go` (add `ListUsers`)
- Modify: `services/e2e-test/internal/actors/client_actor.go` (`Run` line 28-46, `verifyRideInList` lines 87-102, new `verifyUserInList`)
- Modify: `services/e2e-test/internal/actors/driver_actor.go` (`Run` lines 31-65, new `verifyShiftInList`)

**Interfaces:**
- Consumes: `contracts.PagedResponse`, `contracts.UserDto` (Task 1); the four paged endpoints (Tasks 2-5); `Deps.Auth/.Driver/.Ride` clients and `record`/`Verify` from `internal/actors/common.go` (unchanged).
- Produces: `AuthClient.ListUsers(ctx, page, pageSize int) (contracts.PagedResponse[contracts.UserDto], error)`; `RideClient.ListRides` / `DriverClient.ListShifts` now return `contracts.PagedResponse[...]` instead of bare slices. New stats ops: `auth.users.list`, `driver.shift.list`.

- [ ] **Step 1: Update `ListRides`**

In `services/e2e-test/internal/apiclient/ride.go`, replace `ListRides` (lines 35-40) with:

```go
func (c *RideClient) ListRides(ctx context.Context, page, pageSize int) (contracts.PagedResponse[contracts.RideDto], error) {
	var resp contracts.PagedResponse[contracts.RideDto]
	path := fmt.Sprintf("/ride?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}
```

- [ ] **Step 2: Update `ListShifts` and the stale comment**

In `services/e2e-test/internal/apiclient/driver.go`, replace the type comment (lines 11-14) with:

```go
// DriverClient calls driver-service. All GET endpoints return the camelCase
// contracts DTOs (list endpoints wrap them in PagedResponse).
```

and replace `ListShifts` (lines 55-60) with:

```go
func (c *DriverClient) ListShifts(ctx context.Context, page, pageSize int) (contracts.PagedResponse[contracts.ShiftDto], error) {
	var resp contracts.PagedResponse[contracts.ShiftDto]
	path := fmt.Sprintf("/driver-shift?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}
```

- [ ] **Step 3: Add `ListUsers`**

In `services/e2e-test/internal/apiclient/auth.go`: add `"fmt"` to imports, then append:

```go
func (c *AuthClient) ListUsers(ctx context.Context, page, pageSize int) (contracts.PagedResponse[contracts.UserDto], error) {
	var resp contracts.PagedResponse[contracts.UserDto]
	path := fmt.Sprintf("/users?page=%d&pageSize=%d", page, pageSize)
	err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp)
	return resp, err
}
```

- [ ] **Step 4: Update ClientActor**

In `services/e2e-test/internal/actors/client_actor.go`:

(a) In `Run`, right after the `if acc == nil { return }` guard (line 32), add a one-time users-list verification (done immediately after signup so the fresh account is guaranteed on page 1 of the createdAt-desc default sort):

```go
	a.verifyUserInList(ctx, acc)
```

(b) Replace `verifyRideInList` (lines 87-102) with:

```go
func (a *ClientActor) verifyRideInList(ctx context.Context, rideID string) {
	start := time.Now()
	resp, err := a.Ride.ListRides(ctx, 1, 50)
	v := &Verify{}
	if err == nil {
		found := false
		for _, r := range resp.Items {
			if r.ID == rideID {
				found = true
				break
			}
		}
		v.True("list", found, fmt.Sprintf("ride %s not in first 50 of GET /ride", rideID))
		v.True("totalCount", resp.TotalCount >= 1, "expected totalCount >= 1")
	}
	a.record(a.ID, "ride.list", start, err, v)
}
```

(c) Append the new method:

```go
func (a *ClientActor) verifyUserInList(ctx context.Context, acc *account) {
	start := time.Now()
	resp, err := a.Auth.ListUsers(ctx, 1, 50)
	v := &Verify{}
	if err == nil {
		found := false
		for _, u := range resp.Items {
			if u.ID == acc.userID {
				found = true
				break
			}
		}
		v.True("list", found, fmt.Sprintf("user %s not in first 50 of GET /users", acc.userID))
		v.True("totalCount", resp.TotalCount >= 1, "expected totalCount >= 1")
	}
	a.record(a.ID, "auth.users.list", start, err, v)
}
```

- [ ] **Step 5: Update DriverActor**

In `services/e2e-test/internal/actors/driver_actor.go`:

(a) In `Run`, after `a.verifyOpenShift(ctx, shiftID)` (line 47), add (right after opening, the shift is the newest → guaranteed on page 1 of the startedAt-desc default sort):

```go
		if cycle%5 == 0 {
			a.verifyShiftInList(ctx, shiftID)
		}
```

(b) Append the new method:

```go
func (a *DriverActor) verifyShiftInList(ctx context.Context, shiftID string) {
	start := time.Now()
	resp, err := a.Driver.ListShifts(ctx, 1, 50)
	v := &Verify{}
	if err == nil {
		found := false
		for _, s := range resp.Items {
			if s.Id == shiftID {
				found = true
				break
			}
		}
		v.True("list", found, fmt.Sprintf("shift %s not in first 50 of GET /driver-shift", shiftID))
		v.True("totalCount", resp.TotalCount >= 1, "expected totalCount >= 1")
	}
	a.record(a.ID, "driver.shift.list", start, err, v)
}
```

- [ ] **Step 6: Build and vet**

Run (from `services/e2e-test`): `go build ./... && go vet ./...`
Expected: clean. (Runtime verification happens in Task 10 against the full stack.)

- [ ] **Step 7: Commit**

```bash
git add services/e2e-test/internal
git commit -m "e2e-test: adopt PagedResponse envelopes, 1-based pages; verify /users and /driver-shift lists"
```

---

### Task 7: Bruno collection

**Files:**
- Modify: `MyUberGo/Ride Service/Get Ride List.bru`
- Modify: `MyUberGo/Driver Service/Get Driver Profile List.bru`
- Create: `MyUberGo/Driver Service/Get Driver Shift List.bru`
- Create: `MyUberGo/Auth Service/Get Users.bru`

Note: several `.bru` files in this collection have **pre-existing uncommitted edits** — modify on top of the working tree, and `git add` only the four files above.

- [ ] **Step 1: Update `Get Ride List.bru`**

Replace the `get` and `params:query` blocks (keep `meta`, `headers`, `settings` as-is):

```
get {
  url: http://localhost:8001/ride?page=1&pageSize=20&sortBy=createdAt&sortDir=desc
  body: json
  auth: inherit
}

params:query {
  page: 1
  pageSize: 20
  sortBy: createdAt
  sortDir: desc
}
```

- [ ] **Step 2: Update `Get Driver Profile List.bru`**

Open the file (it currently has no query params); set the URL and params (keep the rest of the file as-is):

```
get {
  url: http://localhost:8003/driver-profile?page=1&pageSize=20&sortBy=createdAt&sortDir=desc
  body: json
  auth: inherit
}

params:query {
  page: 1
  pageSize: 20
  sortBy: createdAt
  sortDir: desc
}
```

- [ ] **Step 3: Create `Get Driver Shift List.bru`**

First check existing `seq` values in the folder: `grep -h "seq:" "MyUberGo/Driver Service/"*.bru` — use the next unused integer for `seq` below (shown as 9; bump if taken):

```
meta {
  name: Get Driver Shift List
  type: http
  seq: 9
}

get {
  url: http://localhost:8003/driver-shift?page=1&pageSize=20&sortBy=startedAt&sortDir=desc
  body: json
  auth: inherit
}

params:query {
  page: 1
  pageSize: 20
  sortBy: startedAt
  sortDir: desc
}

settings {
  encodeUrl: true
  timeout: 0
}
```

- [ ] **Step 4: Create `Get Users.bru`**

Check `grep -h "seq:" "MyUberGo/Auth Service/"*.bru`; next unused integer (shown as 4; bump if taken):

```
meta {
  name: Get Users
  type: http
  seq: 4
}

get {
  url: http://localhost:8000/users?page=1&pageSize=20&sortBy=createdAt&sortDir=desc
  body: json
  auth: inherit
}

params:query {
  page: 1
  pageSize: 20
  sortBy: createdAt
  sortDir: desc
}

settings {
  encodeUrl: true
  timeout: 0
}
```

- [ ] **Step 5: Commit (only these four files)**

```bash
git add "MyUberGo/Ride Service/Get Ride List.bru" "MyUberGo/Driver Service/Get Driver Profile List.bru" "MyUberGo/Driver Service/Get Driver Shift List.bru" "MyUberGo/Auth Service/Get Users.bru"
git commit -m "bruno: paging/sort params on list requests; add Get Users and Get Driver Shift List"
```

---

### Task 8: Frontend scaffold — Vite app, proxy, API types + client

**Files:**
- Create: `web/` via scaffold; then `web/vite.config.ts` (replace), `web/src/api/types.ts`, `web/src/api/client.ts`
- Delete: `web/src/App.css`, `web/src/assets/` (scaffold boilerplate; `App.tsx`/`main.tsx`/`index.css` are replaced in Tasks 9-10)

**Interfaces:**
- Produces (Tasks 9-10 rely on these): `PagedResponse<T>{items, page, pageSize, totalCount}`, `SortDir = 'asc'|'desc'`, `PageParams{page, pageSize, sortBy, sortDir}`, `UserDto`, `DriverProfileDto`, `ShiftDto`, `RideDto`, `LocationDto` (mirroring contracts json tags exactly); `fetchPaged<T>(path, params, signal?): Promise<PagedResponse<T>>`. Proxy paths `/api/auth|ride|driver`.

- [ ] **Step 1: Scaffold**

From the repo root:

```bash
npm create vite@latest web -- --template react-ts
cd web
npm install
npm install react-router-dom
```

Then delete boilerplate: `web/src/App.css` and the `web/src/assets/` directory (and the `import './App.css'` / logo usages die when `App.tsx` is replaced in Task 10).

- [ ] **Step 2: Configure the dev proxy**

Replace `web/vite.config.ts`:

```ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Proxy the four service APIs so the browser stays same-origin (the Go
// services have no CORS handling, by design — see CLAUDE.md).
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api/auth': { target: 'http://localhost:8000', changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/auth/, '') },
      '/api/ride': { target: 'http://localhost:8001', changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/ride/, '') },
      '/api/driver': { target: 'http://localhost:8003', changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/driver/, '') },
    },
  },
});
```

- [ ] **Step 3: API types**

Create `web/src/api/types.ts` (field names mirror `services/contracts/http` json tags exactly — keep in sync when contracts change):

```ts
export interface PagedResponse<T> {
  items: T[];
  page: number;
  pageSize: number;
  totalCount: number;
}

export type SortDir = 'asc' | 'desc';

export interface PageParams {
  page: number;
  pageSize: number;
  sortBy: string;
  sortDir: SortDir;
}

export interface UserDto {
  id: string;
  email: string;
  name: string;
  phone: string;
  role: 'Client' | 'Driver';
  createdAt: string;
}

export interface DriverProfileDto {
  id: string;
  userId: string;
  driverName: string;
  phone: string;
  rating: number;
  vehicleType: string;
  licencePlate: string;
  status: string;
  totalRidesCompleted: number;
  createdAt: string;
}

export interface ShiftDto {
  id: string;
  driverId: string;
  startedAt: string;
  endedAt?: string | null;
  totalRides: number;
  totalEarnings: number;
}

export interface LocationDto {
  latitude: number;
  longitude: number;
  address: string;
}

export interface RideDto {
  id: string;
  clientId: string;
  driverId?: string | null;
  status: string;
  pickup: LocationDto;
  destination: LocationDto;
  estimatedPrice: number;
  estimatedDistanceKm: number;
  createdAt: string;
}
```

- [ ] **Step 4: Fetch client**

Create `web/src/api/client.ts`:

```ts
import type { PagedResponse, PageParams } from './types';

export async function fetchPaged<T>(
  path: string,
  params: PageParams,
  signal?: AbortSignal,
): Promise<PagedResponse<T>> {
  const qs = new URLSearchParams({
    page: String(params.page),
    pageSize: String(params.pageSize),
    sortBy: params.sortBy,
    sortDir: params.sortDir,
  });

  const res = await fetch(`${path}?${qs}`, { signal });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json() as Promise<PagedResponse<T>>;
}
```

- [ ] **Step 5: Keep the app compiling with a placeholder**

Deleting `App.css`/`assets/` breaks the scaffolded `App.tsx` imports, so replace `web/src/App.tsx` with a placeholder (Task 10 writes the real one):

```tsx
export function App() {
  return <p>MyUberGo admin — pages arrive in a later task.</p>;
}

export default App;
```

Leave `web/src/main.tsx` and `web/src/index.css` as scaffolded for now (both are replaced in Task 10).

Run (from `web/`): `npx tsc -b`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add web
git commit -m "web: scaffold Vite+React+TS admin dashboard; dev proxy, DTO types, fetchPaged client"
```

(If `web/.gitignore` from the scaffold doesn't exist, ensure `node_modules/` and `dist/` are ignored before committing.)

---

### Task 9: Frontend — `usePagedQuery` hook, `DataTable`, `Pagination`

**Files:**
- Create: `web/src/hooks/usePagedQuery.ts`, `web/src/components/DataTable.tsx`, `web/src/components/Pagination.tsx`

**Interfaces:**
- Consumes: `fetchPaged`, types from Task 8; `useSearchParams` from react-router-dom (router mounted in Task 10 — components compile standalone).
- Produces (Task 10 relies on these): `usePagedQuery<T>(path, defaults: {sortBy, sortDir?, pageSize?}) → {data, loading, error, params, setPage(page), toggleSort(key)}`; `Column<T>{key, header, sortable?, render}`; `DataTable<T>({columns, rows, rowKey, sortBy, sortDir, onSort, loading?})`; `Pagination({page, pageSize, totalCount, onPageChange})`.

- [ ] **Step 1: The hook**

Create `web/src/hooks/usePagedQuery.ts`:

```ts
import { useEffect, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { fetchPaged } from '../api/client';
import type { PagedResponse, PageParams, SortDir } from '../api/types';

export interface UsePagedQueryResult<T> {
  data: PagedResponse<T> | null;
  loading: boolean;
  error: string | null;
  params: PageParams;
  setPage: (page: number) => void;
  toggleSort: (sortBy: string) => void;
}

// Server-side paged + sorted fetch whose page/sort state lives in the URL
// query string, so every table view is bookmarkable and survives refresh.
export function usePagedQuery<T>(
  path: string,
  defaults: { sortBy: string; sortDir?: SortDir; pageSize?: number },
): UsePagedQueryResult<T> {
  const [searchParams, setSearchParams] = useSearchParams();

  const params: PageParams = {
    page: Math.max(1, Number(searchParams.get('page') ?? '1') || 1),
    pageSize: defaults.pageSize ?? 20,
    sortBy: searchParams.get('sortBy') ?? defaults.sortBy,
    sortDir: (searchParams.get('sortDir') as SortDir | null) ?? defaults.sortDir ?? 'desc',
  };

  const [data, setData] = useState<PagedResponse<T> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    fetchPaged<T>(path, params, controller.signal)
      .then(setData)
      .catch((err: unknown) => {
        if (!controller.signal.aborted) {
          setError(err instanceof Error ? err.message : String(err));
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, params.page, params.pageSize, params.sortBy, params.sortDir]);

  const setPage = (page: number) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.set('page', String(page));
      return next;
    });
  };

  // Same column: flip direction. New column: sort by it descending. Either
  // way jump back to page 1 — the old offset is meaningless under a new order.
  const toggleSort = (sortBy: string) => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      if (params.sortBy === sortBy) {
        next.set('sortDir', params.sortDir === 'desc' ? 'asc' : 'desc');
      } else {
        next.set('sortBy', sortBy);
        next.set('sortDir', 'desc');
      }
      next.set('page', '1');
      return next;
    });
  };

  return { data, loading, error, params, setPage, toggleSort };
}
```

- [ ] **Step 2: The table**

Create `web/src/components/DataTable.tsx`:

```tsx
import type { ReactNode } from 'react';
import type { SortDir } from '../api/types';

export interface Column<T> {
  /** API sort key — must match the endpoint's Go sort whitelist. */
  key: string;
  header: string;
  sortable?: boolean;
  render: (row: T) => ReactNode;
}

export interface DataTableProps<T> {
  columns: Column<T>[];
  rows: T[];
  rowKey: (row: T) => string;
  sortBy: string;
  sortDir: SortDir;
  onSort: (key: string) => void;
  loading?: boolean;
}

export function DataTable<T>({ columns, rows, rowKey, sortBy, sortDir, onSort, loading }: DataTableProps<T>) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          {columns.map((col) => (
            <th key={col.key}>
              {col.sortable ? (
                <button type="button" className="sort-header" onClick={() => onSort(col.key)}>
                  {col.header}
                  {sortBy === col.key ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ''}
                </button>
              ) : (
                col.header
              )}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.length === 0 ? (
          <tr>
            <td colSpan={columns.length} className="empty">
              {loading ? 'Loading…' : 'No data'}
            </td>
          </tr>
        ) : (
          rows.map((row) => (
            <tr key={rowKey(row)}>
              {columns.map((col) => (
                <td key={col.key}>{col.render(row)}</td>
              ))}
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 3: The pagination**

Create `web/src/components/Pagination.tsx`:

```tsx
export interface PaginationProps {
  page: number;
  pageSize: number;
  totalCount: number;
  onPageChange: (page: number) => void;
}

export function Pagination({ page, pageSize, totalCount, onPageChange }: PaginationProps) {
  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const from = Math.max(1, page - 2);
  const to = Math.min(totalPages, page + 2);
  const pages: number[] = [];
  for (let p = from; p <= to; p++) pages.push(p);

  return (
    <nav className="pagination">
      <button type="button" disabled={page <= 1} onClick={() => onPageChange(page - 1)}>
        ‹ Prev
      </button>
      {from > 1 && (
        <>
          <button type="button" onClick={() => onPageChange(1)}>1</button>
          {from > 2 && <span>…</span>}
        </>
      )}
      {pages.map((p) => (
        <button key={p} type="button" disabled={p === page} className={p === page ? 'current' : ''} onClick={() => onPageChange(p)}>
          {p}
        </button>
      ))}
      {to < totalPages && (
        <>
          {to < totalPages - 1 && <span>…</span>}
          <button type="button" onClick={() => onPageChange(totalPages)}>{totalPages}</button>
        </>
      )}
      <button type="button" disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
        Next ›
      </button>
      <span className="total">{totalCount} rows</span>
    </nav>
  );
}
```

- [ ] **Step 4: Type-check and commit**

Run (from `web/`): `npx tsc -b`
Expected: clean.

```bash
git add web/src
git commit -m "web: usePagedQuery hook (URL-backed state), DataTable and Pagination components"
```

---

### Task 10: Frontend — pages, nav, styles; full-app verification

**Files:**
- Create: `web/src/pages/UsersPage.tsx`, `web/src/pages/DriversPage.tsx`, `web/src/pages/ShiftsPage.tsx`, `web/src/pages/RidesPage.tsx`
- Modify: `web/src/App.tsx` (replace placeholder), `web/src/main.tsx` (router), `web/src/index.css` (replace scaffold styles), `web/index.html` (title)

**Interfaces:**
- Consumes: everything from Tasks 8-9. Sortable columns per page = exactly the Go whitelists (Users: `email/name/role/createdAt`; Drivers: `driverName/rating/status/vehicleType/totalRidesCompleted/createdAt`; Shifts: `startedAt/endedAt/totalRides/totalEarnings`; Rides: `status/estimatedPrice/estimatedDistanceKm/createdAt`).

- [ ] **Step 1: Shared page shape — UsersPage**

Create `web/src/pages/UsersPage.tsx`:

```tsx
import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { UserDto } from '../api/types';

const columns: Column<UserDto>[] = [
  { key: 'id', header: 'ID', render: (u) => <code title={u.id}>{u.id.slice(0, 8)}</code> },
  { key: 'email', header: 'Email', sortable: true, render: (u) => u.email },
  { key: 'name', header: 'Name', sortable: true, render: (u) => u.name },
  { key: 'phone', header: 'Phone', render: (u) => u.phone },
  { key: 'role', header: 'Role', sortable: true, render: (u) => u.role },
  { key: 'createdAt', header: 'Created', sortable: true, render: (u) => new Date(u.createdAt).toLocaleString() },
];

export function UsersPage() {
  const q = usePagedQuery<UserDto>('/api/auth/users', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Users</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(u) => u.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
```

- [ ] **Step 2: DriversPage**

Create `web/src/pages/DriversPage.tsx`:

```tsx
import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { DriverProfileDto } from '../api/types';

const columns: Column<DriverProfileDto>[] = [
  { key: 'id', header: 'ID', render: (d) => <code title={d.id}>{d.id.slice(0, 8)}</code> },
  { key: 'driverName', header: 'Name', sortable: true, render: (d) => d.driverName },
  { key: 'phone', header: 'Phone', render: (d) => d.phone },
  { key: 'rating', header: 'Rating', sortable: true, render: (d) => d.rating.toFixed(1) },
  { key: 'vehicleType', header: 'Vehicle', sortable: true, render: (d) => d.vehicleType },
  { key: 'licencePlate', header: 'Plate', render: (d) => d.licencePlate },
  { key: 'status', header: 'Status', sortable: true, render: (d) => d.status },
  { key: 'totalRidesCompleted', header: 'Rides', sortable: true, render: (d) => d.totalRidesCompleted },
  { key: 'createdAt', header: 'Created', sortable: true, render: (d) => new Date(d.createdAt).toLocaleString() },
];

export function DriversPage() {
  const q = usePagedQuery<DriverProfileDto>('/api/driver/driver-profile', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Driver Profiles</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(d) => d.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
```

- [ ] **Step 3: ShiftsPage**

Create `web/src/pages/ShiftsPage.tsx`:

```tsx
import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { ShiftDto } from '../api/types';

const columns: Column<ShiftDto>[] = [
  { key: 'id', header: 'ID', render: (s) => <code title={s.id}>{s.id.slice(0, 8)}</code> },
  { key: 'driverId', header: 'Driver', render: (s) => <code title={s.driverId}>{s.driverId.slice(0, 8)}</code> },
  { key: 'startedAt', header: 'Started', sortable: true, render: (s) => new Date(s.startedAt).toLocaleString() },
  { key: 'endedAt', header: 'Ended', sortable: true, render: (s) => (s.endedAt ? new Date(s.endedAt).toLocaleString() : '—') },
  { key: 'totalRides', header: 'Rides', sortable: true, render: (s) => s.totalRides },
  { key: 'totalEarnings', header: 'Earnings', sortable: true, render: (s) => s.totalEarnings.toFixed(2) },
];

export function ShiftsPage() {
  const q = usePagedQuery<ShiftDto>('/api/driver/driver-shift', { sortBy: 'startedAt' });
  return (
    <section>
      <h1>Shifts</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(s) => s.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
```

- [ ] **Step 4: RidesPage**

Create `web/src/pages/RidesPage.tsx`:

```tsx
import { usePagedQuery } from '../hooks/usePagedQuery';
import { DataTable, type Column } from '../components/DataTable';
import { Pagination } from '../components/Pagination';
import type { RideDto } from '../api/types';

const columns: Column<RideDto>[] = [
  { key: 'id', header: 'ID', render: (r) => <code title={r.id}>{r.id.slice(0, 8)}</code> },
  { key: 'clientId', header: 'Client', render: (r) => <code title={r.clientId}>{r.clientId.slice(0, 8)}</code> },
  { key: 'driverId', header: 'Driver', render: (r) => (r.driverId ? <code title={r.driverId}>{r.driverId.slice(0, 8)}</code> : '—') },
  { key: 'status', header: 'Status', sortable: true, render: (r) => r.status },
  { key: 'pickup', header: 'Pickup', render: (r) => r.pickup.address },
  { key: 'destination', header: 'Destination', render: (r) => r.destination.address },
  { key: 'estimatedPrice', header: 'Price', sortable: true, render: (r) => r.estimatedPrice.toFixed(2) },
  { key: 'estimatedDistanceKm', header: 'Km', sortable: true, render: (r) => r.estimatedDistanceKm.toFixed(2) },
  { key: 'createdAt', header: 'Created', sortable: true, render: (r) => new Date(r.createdAt).toLocaleString() },
];

export function RidesPage() {
  const q = usePagedQuery<RideDto>('/api/ride/ride', { sortBy: 'createdAt' });
  return (
    <section>
      <h1>Rides</h1>
      {q.error && <p className="error">{q.error}</p>}
      <DataTable columns={columns} rows={q.data?.items ?? []} rowKey={(r) => r.id} sortBy={q.params.sortBy} sortDir={q.params.sortDir} onSort={q.toggleSort} loading={q.loading} />
      <Pagination page={q.params.page} pageSize={q.params.pageSize} totalCount={q.data?.totalCount ?? 0} onPageChange={q.setPage} />
    </section>
  );
}
```

- [ ] **Step 5: App shell + router + styles**

Replace `web/src/App.tsx`:

```tsx
import { NavLink, Outlet } from 'react-router-dom';

export function App() {
  return (
    <div className="layout">
      <nav className="topnav">
        <span className="brand">MyUberGo admin</span>
        <NavLink to="/users">Users</NavLink>
        <NavLink to="/drivers">Drivers</NavLink>
        <NavLink to="/shifts">Shifts</NavLink>
        <NavLink to="/rides">Rides</NavLink>
      </nav>
      <main>
        <Outlet />
      </main>
    </div>
  );
}

export default App;
```

Replace `web/src/main.tsx`:

```tsx
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';
import App from './App';
import { UsersPage } from './pages/UsersPage';
import { DriversPage } from './pages/DriversPage';
import { ShiftsPage } from './pages/ShiftsPage';
import { RidesPage } from './pages/RidesPage';
import './index.css';

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <Routes>
        <Route element={<App />}>
          <Route index element={<Navigate to="/rides" replace />} />
          <Route path="/users" element={<UsersPage />} />
          <Route path="/drivers" element={<DriversPage />} />
          <Route path="/shifts" element={<ShiftsPage />} />
          <Route path="/rides" element={<RidesPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  </StrictMode>,
);
```

Replace `web/src/index.css`:

```css
:root {
  font-family: system-ui, sans-serif;
  color-scheme: light dark;
}

body {
  margin: 0;
}

.layout {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 1rem 2rem;
}

.topnav {
  display: flex;
  gap: 1rem;
  align-items: center;
  padding: 0.75rem 0;
  border-bottom: 1px solid color-mix(in srgb, currentColor 20%, transparent);
}

.topnav .brand {
  font-weight: 700;
  margin-right: 1rem;
}

.topnav a {
  text-decoration: none;
  color: inherit;
  opacity: 0.7;
}

.topnav a.active {
  opacity: 1;
  font-weight: 600;
  text-decoration: underline;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.9rem;
}

.data-table th,
.data-table td {
  text-align: left;
  padding: 0.4rem 0.6rem;
  border-bottom: 1px solid color-mix(in srgb, currentColor 15%, transparent);
  white-space: nowrap;
}

.data-table td.empty {
  text-align: center;
  padding: 2rem;
  opacity: 0.6;
}

.sort-header {
  background: none;
  border: none;
  padding: 0;
  font: inherit;
  font-weight: 600;
  cursor: pointer;
  color: inherit;
}

.pagination {
  display: flex;
  gap: 0.25rem;
  align-items: center;
  margin-top: 0.75rem;
}

.pagination button {
  min-width: 2rem;
  padding: 0.25rem 0.5rem;
  cursor: pointer;
}

.pagination button.current {
  font-weight: 700;
}

.pagination .total {
  margin-left: auto;
  opacity: 0.6;
  font-size: 0.85rem;
}

.error {
  color: #c0392b;
}
```

In `web/index.html`, set `<title>MyUberGo admin</title>`.

- [ ] **Step 6: Build and verify against the stack**

```bash
cd web && npx tsc -b && npm run build
```
Expected: clean type-check and production build.

Then with the stack up (`docker-compose up --build` from repo root, in another terminal): `npm run dev`, open http://localhost:5173 and check all four screens render data, every sortable header toggles ▲/▼ and reorders rows, paging works, and a URL like `/rides?page=2&sortBy=estimatedPrice&sortDir=asc` survives refresh.

- [ ] **Step 7: Commit**

```bash
git add web
git commit -m "web: Users/Drivers/Shifts/Rides pages with routed nav and URL-backed paging/sorting"
```

---

### Task 11: Docs + end-to-end verification of the whole feature

**Files:**
- Modify: `CLAUDE.md` (add `web/` section; update "no `*_test.go`" sentence in Commands)
- Modify: `PLAN.md` (append a 2026-07-18 entry for this feature)

- [ ] **Step 1: Full-stack verification (Git Bash, stack running via `docker-compose up --build`)**

```bash
# envelope + sorting + no password_hash
curl -s 'localhost:8000/users?page=1&pageSize=5&sortBy=email&sortDir=asc' | grep -c password_hash   # expect 0
curl -s 'localhost:8000/users?page=1&pageSize=5' | head -c 400                                      # expect {"items":[...],"page":1,...}

# ride off-by-one fixed: newest ride must appear on page 1
curl -s 'localhost:8001/ride?page=1&pageSize=5' | head -c 400
curl -s -o /dev/null -w '%{http_code}\n' 'localhost:8001/ride?page=0'          # expect 400
curl -s -o /dev/null -w '%{http_code}\n' 'localhost:8001/ride?sortBy=bogus'    # expect 400

# driver-service camelCase + shift shape (no "Status", RFC3339, endedAt null omitted)
curl -s 'localhost:8003/driver-profile?sortBy=rating&sortDir=desc' | head -c 400   # expect "driverName", not "DriverName"
curl -s 'localhost:8003/driver-shift?sortBy=totalEarnings&sortDir=desc' | head -c 400

# totalCount cross-check
docker exec -i $(docker ps -qf name=postgres) psql -U postgres -c 'SELECT COUNT(*) FROM ride.ride;'
```

Then run the simulator (from `services/e2e-test`): `go run ./cmd` for a few minutes — expect all ops green including `auth.users.list`, `driver.shift.list`, `ride.list`, with no verify failures. Re-run the four Bruno list requests. Browse the dashboard once more while the simulator writes (row counts should grow on refresh).

- [ ] **Step 2: Update CLAUDE.md**

(a) In the Commands section, replace the sentence

> There are currently no `*_test.go` files in any service — don't assume a test suite exists; if you add one, `go test ./...` from within the service directory is the way to run it.

with

> Unit tests exist for paging/sorting param parsing and DTO mapping (`go test ./...` from within the service directory); there is no integration-test suite — the e2e-test simulator is the de facto integration check.

(b) After the "API collection" section, add:

```markdown
## Admin dashboard

`web/` at the repo root is a Vite + React + TypeScript read-only admin dashboard (Users / Drivers / Shifts / Rides tables with server-side paging + sorting via the shared `PagedResponse[T]` envelope in `contracts/http`). Deliberately **not** in docker-compose — run it manually against the running stack:

```bash
cd web
npm install
npm run dev   # http://localhost:5173
```

The Vite dev server proxies `/api/auth` → :8000, `/api/ride` → :8001, `/api/driver` → :8003 (`web/vite.config.ts`), so the Go services need no CORS headers — keep it that way. TypeScript DTOs in `web/src/api/types.ts` mirror `services/contracts/http` json tags — update them whenever contracts change. List-endpoint paging contract (all services): 1-based `page`, `pageSize` (default 20, cap 100), `sortBy` validated against a per-endpoint whitelist, `sortDir` asc|desc.
```

(c) In the "Target vs. current schema/contracts" section, note that list endpoints now return `PagedResponse[T]` (bare arrays are gone) and that driver-service GET endpoints now serve camelCase contracts DTOs.

- [ ] **Step 3: Update PLAN.md**

Append a dated section (match the existing entry style in the file):

```markdown
## 2026-07-18 — Admin dashboard + paged/sorted list endpoints

- [x] `contracts/http`: `PagedResponse[T]` envelope, `UserDto`; dropped phantom `ShiftDto.Status`; `endedAt` nullable
- [x] auth-service: new `GET /users` (paged/sorted, never exposes password_hash)
- [x] ride-service: fixed 1-based paging off-by-one (first page was skipped), sort whitelist, totalCount
- [x] driver-service: `PageRequest` + sort whitelists through query/persistence; count queries; GET endpoints normalized to camelCase DTOs with RFC3339 timestamps
- [x] e2e-test: envelope adoption; new `auth.users.list` / `driver.shift.list` verifies
- [x] Bruno: list requests updated; `Get Users`, `Get Driver Shift List` added
- [x] `web/`: Vite+React+TS dashboard (DataTable/Pagination/usePagedQuery, URL-backed state, dev-proxy → no CORS)
```

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md PLAN.md
git commit -m "docs: admin dashboard + paging/sorting contract in CLAUDE.md; PLAN.md progress entry"
```
