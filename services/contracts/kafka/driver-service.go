package contracts

type ShiftUpdatedEvent struct {
	ShiftID   string  `json:"shiftId"`
	DriverID  string  `json:"driverId"`
	Status    string  `json:"status"`
	Rating    float64 `json:"rating"`
	UpdatedAt string  `json:"updatedAt"`
}
