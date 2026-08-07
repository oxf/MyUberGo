package paging

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// ListParams is a validated set of paging/sorting query params. SortBy is an
// API sort key already checked against the entity's domain sort-column map
// (persistence does the column mapping); SortDir is "ASC" or "DESC".
type ListParams struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

func ParseListParams(r *http.Request, sortColumns map[string]string, defaultSortKey string) (ListParams, error) {
	page, err := parseIntQuery(r, "page", 1)
	if err != nil {
		return ListParams{}, err
	}
	if page < 1 {
		return ListParams{}, fmt.Errorf("page must be >= 1")
	}

	pageSize, err := parseIntQuery(r, "pageSize", 20)
	if err != nil {
		return ListParams{}, err
	}
	if pageSize < 1 {
		return ListParams{}, fmt.Errorf("pageSize must be >= 1")
	}
	if pageSize > 100 {
		return ListParams{}, fmt.Errorf("pageSize cannot exceed 100")
	}

	sortBy := r.URL.Query().Get("sortBy")
	if sortBy == "" {
		sortBy = defaultSortKey
	}
	if _, ok := sortColumns[sortBy]; !ok {
		return ListParams{}, fmt.Errorf("unknown sortBy %q", sortBy)
	}

	sortDir := strings.ToLower(r.URL.Query().Get("sortDir"))
	switch sortDir {
	case "":
		sortDir = "desc"
	case "asc", "desc":
	default:
		return ListParams{}, fmt.Errorf("sortDir must be asc or desc")
	}

	return ListParams{Page: page, PageSize: pageSize, SortBy: sortBy, SortDir: strings.ToUpper(sortDir)}, nil
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
