package contracts

type AddPaymentMethodRequest struct {
	ProviderPaymentMethodId string `json:"providerPaymentMethodId"`
	Brand                   string `json:"brand"`
	Last4                   string `json:"last4"`
	ExpMonth                int    `json:"expMonth"`
	ExpYear                 int    `json:"expYear"`
	SetDefault              bool   `json:"setDefault"`
}

type AddPaymentMethodResponse struct {
	Id string `json:"id"`
}

type PaymentMethodDto struct {
	Id        string `json:"id"`
	Brand     string `json:"brand"`
	Last4     string `json:"last4"`
	ExpMonth  int    `json:"expMonth"`
	ExpYear   int    `json:"expYear"`
	IsDefault bool   `json:"isDefault"`
	Status    string `json:"status"`
	CreatedAt string `json:"createdAt"`
}

type InvoiceLineDto struct {
	Kind        string `json:"kind"`
	AmountMinor int64  `json:"amountMinor"`
	Currency    string `json:"currency"`
	Description string `json:"description"`
}

type InvoiceDto struct {
	Id           string           `json:"id"`
	RideId       string           `json:"rideId"`
	ClientId     string           `json:"clientId"`
	DriverId     *string          `json:"driverId,omitempty"`
	Type         string           `json:"type"`
	Status       string           `json:"status"`
	AmountMinor  int64            `json:"amountMinor"`
	Currency     string           `json:"currency"`
	AttemptCount int              `json:"attemptCount"`
	Lines        []InvoiceLineDto `json:"lines"`
	CreatedAt    string           `json:"createdAt"`
	PaidAt       *string          `json:"paidAt,omitempty"`
}
