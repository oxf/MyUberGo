package contracts

type ShiftUpdatedEvent struct {
	ShiftID   string `json:"shiftId"`
	DriverID  string `json:"driverId"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updatedAt"`
}
