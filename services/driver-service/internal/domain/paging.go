package domain

import "github.com/oxf/MyUber/common/paging"

// PageRequest carries validated list-query paging/sorting. Page is 1-based.
// SortBy is a key of the entity's sort-column map and SortDir is "ASC" or
// "DESC" — the HTTP layer validates both before building a PageRequest.
type PageRequest = paging.PageRequest

// Sort whitelists: API sort key -> SQL column. ORDER BY cannot be
// parameterized, so persistence interpolates only values from these maps.
var DriverSortColumns = map[string]string{
	"createdAt":           "created_at",
	"rating":              "rating",
	"status":              "status",
	"vehicleType":         "vehicle_type",
	"totalRidesCompleted": "total_rides_completed",
}

var ShiftSortColumns = map[string]string{
	"startedAt":          "started_at",
	"endedAt":            "ended_at",
	"totalRides":         "total_rides",
	"totalEarningsMinor": "total_earnings_minor",
}
