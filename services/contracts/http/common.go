package contracts

// PagedResponse is the shared list-endpoint envelope. Pages are 1-based;
// TotalCount is the unpaged row count so clients can render page numbers.
type PagedResponse[T any] struct {
	Items      []T `json:"items"`
	Page       int `json:"page"`
	PageSize   int `json:"pageSize"`
	TotalCount int `json:"totalCount"`
}
