package domain

type Client struct {
	ID                  string
	UserID              string
	Rating              float64
	TotalRidesCompleted int
	CreatedAt           string // RFC3339
}
