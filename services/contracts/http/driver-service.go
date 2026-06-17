package contracts

type DriverProfileDto struct {
	Id                  string  `json:"id"`
	UserId              string  `json:"userId"`
	DriverName          string  `json:"driverName"`
	Phone               string  `json:"phone"`
	Rating              float64 `json:"rating"`
	VehicleType         string  `json:"vehicleType"`
	LicencePlate        string  `json:"licencePlate"`
	Status              string  `json:"status"`
	TotalRidesCompleted int     `json:"totalRidesCompleted"`
	CreatedAt           string  `json:"createdAt"`
}

type CreateDriverProfileDto struct {
	UserId       string `json:"userId"`
	DriverName   string `json:"driverName"`
	Phone        string `json:"phone"`
	VehicleType  string `json:"vehicleType"`
	LicencePlate string `json:"licencePlate"`
}

type CreateDriverProfileResponse struct {
	Id string `json:"id"`
}

type UpdateDriverProfileDto struct {
	UserId       string `json:"userId"`
	DriverName   string `json:"driverName"`
	Phone        string `json:"phone"`
	VehicleType  string `json:"vehicleType"`
	LicencePlate string `json:"licencePlate"`
}

type ShiftDto struct {
	Id            string  `json:"id"`
	DriverId      string  `json:"driverId"`
	Status        string  `json:"status"`
	StartedAt     string  `json:"startedAt"`
	EndedAt       string  `json:"endedAt"`
	TotalRides    int     `json:"totalRides"`
	TotalEarnings float64 `json:"totalEarnings"`
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
