package query

type PagedResult[T any] struct {
	Items      []T
	TotalCount int
}
