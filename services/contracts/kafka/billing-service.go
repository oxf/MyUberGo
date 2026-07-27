package contracts

type PaymentCompletedEvent struct {
	RideID             string `json:"rideId"`
	InvoiceID          string `json:"invoiceId"`
	ClientID           string `json:"clientId"`
	DriverID           string `json:"driverId,omitempty"`
	AmountMinor        int64  `json:"amountMinor"`
	PlatformFeeMinor   int64  `json:"platformFeeMinor"`
	DriverPayableMinor int64  `json:"driverPayableMinor"`
	Currency           string `json:"currency"`
	PaidAt             string `json:"paidAt"`
}

type PaymentFailedEvent struct {
	RideID      string `json:"rideId"`
	InvoiceID   string `json:"invoiceId"`
	ClientID    string `json:"clientId"`
	AmountMinor int64  `json:"amountMinor"`
	Currency    string `json:"currency"`
	FailureCode string `json:"failureCode"`
	FailedAt    string `json:"failedAt"`
}
