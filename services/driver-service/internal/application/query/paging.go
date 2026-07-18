package query

// PagedResult pairs one page of items with the unpaged total, so the HTTP
// layer can build the wire envelope without a second query round-trip.
type PagedResult[T any] struct {
	Items      []T
	TotalCount int
}
