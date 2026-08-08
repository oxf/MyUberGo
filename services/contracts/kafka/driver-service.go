package contracts

type ShiftUpdatedEvent struct {
	ShiftID  string  `json:"shiftId"`
	DriverID string  `json:"driverId"`
	Status   string  `json:"status"`
	Rating   float64 `json:"rating"`
	// UserID is the driver's auth.user(id) — threaded through so matching-service
	// can verify a caller's X-User-Id against the driverId they're acting on.
	UserID    string `json:"userId,omitempty"`
	UpdatedAt string `json:"updatedAt"`
}
