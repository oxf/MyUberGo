package paging

// PageRequest carries validated list-query paging/sorting. Page is 1-based.
// SortBy is a key of the entity's sort-column map and SortDir is "ASC" or
// "DESC" — the HTTP layer validates both before building a PageRequest.
type PageRequest struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}
