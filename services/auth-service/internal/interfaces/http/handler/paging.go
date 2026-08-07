package handler

import (
	"net/http"

	"github.com/oxf/MyUber/common/paging"
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
	p, err := paging.ParseListParams(r, sortColumns, defaultSortKey)
	if err != nil {
		return listParams{}, err
	}
	return listParams{page: p.Page, pageSize: p.PageSize, sortBy: p.SortBy, sortDir: p.SortDir}, nil
}
