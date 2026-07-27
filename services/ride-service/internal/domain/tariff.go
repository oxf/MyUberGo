package domain

import "context"

type Tariff struct {
	ID               string
	Name             string
	BaseFareMinor    int64
	PricePerKmMinor  int64
	PricePerMinMinor int64
	Currency         string
}

// TariffRepository reads fare tables. There is no write path today —
// ride.tariff rows are seeded by init.sql, not managed via the API.
type TariffRepository interface {
	GetByName(ctx context.Context, name string) (*Tariff, error)
}
