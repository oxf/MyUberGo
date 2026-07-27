package handler

import (
	"fmt"
	"net/http"
	"strconv"
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
