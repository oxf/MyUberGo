package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"
	"errors"

	"github.com/sirupsen/logrus"
)

type UpdateDriver struct {
	ID           string
	VehicleType  string
	LicencePlate string
}

type UpdateDriverHandler struct {
	repo domain.DriverRepository
}

func NewUpdateDriverHandler(repo domain.DriverRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandlerNoResult[UpdateDriver] {
	handler := &UpdateDriverHandler{repo: repo}
	return decorator.ApplyCommandDecoratorsNoResult[UpdateDriver](handler, logger, metricsClient)
}

func (h *UpdateDriverHandler) Handle(ctx context.Context, cmd UpdateDriver) error {
	if cmd.VehicleType == "" && cmd.LicencePlate == "" {
		return errors.New("nothing to update")
	}
	return h.repo.UpdateDriver(ctx, cmd.ID, cmd.VehicleType, cmd.LicencePlate)
}
