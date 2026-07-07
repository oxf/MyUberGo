package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"
	"errors"

	"github.com/sirupsen/logrus"
)

type UpdateDriverProfile struct {
	ID           string
	DriverName   string
	Phone        string
	VehicleType  string
	LicencePlate string
}

type UpdateDriverProfileHandler struct {
	repo domain.DriverProfileRepository
}

func NewUpdateDriverProfileHandler(repo domain.DriverProfileRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandlerNoResult[UpdateDriverProfile] {
	handler := &UpdateDriverProfileHandler{repo: repo}
	return decorator.ApplyCommandDecoratorsNoResult[UpdateDriverProfile](handler, logger, metricsClient)
}

func (h *UpdateDriverProfileHandler) Handle(ctx context.Context, cmd UpdateDriverProfile) error {
	if cmd.DriverName == "" && cmd.Phone == "" && cmd.VehicleType == "" && cmd.LicencePlate == "" {
		return errors.New("nothing to update")
	}
	return h.repo.UpdateDriverProfile(ctx, cmd.ID, cmd.DriverName, cmd.Phone, cmd.VehicleType, cmd.LicencePlate)
}
