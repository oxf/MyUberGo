package contracts

type DriverDto struct {
	Id                  string  `json:"id"`
	UserId              string  `json:"userId"`
	Rating              float64 `json:"rating"`
	VehicleType         string  `json:"vehicleType"`
	LicencePlate        string  `json:"licencePlate"`
	Status              string  `json:"status"`
	TotalRidesCompleted int     `json:"totalRidesCompleted"`
	CreatedAt           string  `json:"createdAt"`
}

type CreateDriverDto struct {
	UserId       string `json:"userId"`
	VehicleType  string `json:"vehicleType"`
	LicencePlate string `json:"licencePlate"`
}

type CreateDriverResponse struct {
	Id string `json:"id"`
}

type UpdateDriverDto struct {
	VehicleType  string `json:"vehicleType"`
	LicencePlate string `json:"licencePlate"`
}

type ShiftDto struct {
	Id                 string  `json:"id"`
	DriverId           string  `json:"driverId"`
	StartedAt          string  `json:"startedAt"`
	EndedAt            *string `json:"endedAt,omitempty"`
	TotalRides         int     `json:"totalRides"`
	TotalEarningsMinor int64   `json:"totalEarningsMinor"`
	Currency           string  `json:"currency"`
}
type CreateShiftRequest struct {
	DriverId string `json:"driverId"`
}
type CreateShiftResponse struct {
	Id string `json:"id"`
}

type UpdateShiftRequest struct {
	DriverId string `json:"driverId"`
	Status   string `json:"status"`
}
type UpdateShiftResponse struct {
	Id     string `json:"id"`
	Status string `json:"status"`
}
