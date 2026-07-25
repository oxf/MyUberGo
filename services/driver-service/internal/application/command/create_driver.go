package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type CreateDriver struct {
	UserID       string
	VehicleType  string
	LicencePlate string
}

type CreateDriverResult struct {
	ID string
}

type CreateDriverHandler struct {
	repo domain.DriverRepository
}

func NewCreateDriverHandler(repo domain.DriverRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandler[CreateDriver, CreateDriverResult] {
	if repo == nil {
		panic("nil repo")
	}

	handler := &CreateDriverHandler{repo: repo}
	return decorator.ApplyCommandDecorators[CreateDriver, CreateDriverResult](handler, logger, metricsClient)
}

func (h *CreateDriverHandler) Handle(ctx context.Context, cmd CreateDriver) (CreateDriverResult, error) {
	driver, err := domain.NewDriver(cmd.UserID, cmd.VehicleType, cmd.LicencePlate)
	if err != nil {
		return CreateDriverResult{}, err
	}

	id, err := h.repo.CreateDriver(ctx, driver)
	if err != nil {
		return CreateDriverResult{}, err
	}

	return CreateDriverResult{ID: id}, nil
}
