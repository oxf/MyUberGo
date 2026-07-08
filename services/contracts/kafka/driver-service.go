package contracts

type ShiftUpdatedEvent struct {
	ShiftID   string `json:"shiftId"`
	DriverID  string `json:"clientId"`
	Status    string `json:"status"`
	UpdatedAt string `json:"updatedAt"`
}
