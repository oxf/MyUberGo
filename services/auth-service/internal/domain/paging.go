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

// Sort whitelist: API sort key -> SQL column. ORDER BY cannot be
// parameterized, so persistence interpolates only values from this map.
var UserSortColumns = map[string]string{
	"email":     "email",
	"name":      "name",
	"role":      "role",
	"createdAt": "created_at",
}
