package command

import (
	"context"
	"driver-service/internal/common/decorator"
	"driver-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type CreateDriverProfile struct {
	UserID       string
	DriverName   string
	Phone        string
	VehicleType  string
	LicencePlate string
}

type CreateDriverProfileResult struct {
	ID string
}

type CreateDriverProfileHandler struct {
	repo domain.DriverProfileRepository
}

func NewCreateDriverProfileHandler(repo domain.DriverProfileRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient) decorator.CommandHandler[CreateDriverProfile, CreateDriverProfileResult] {
	if repo == nil {
		panic("nil repo")
	}

	handler := &CreateDriverProfileHandler{repo: repo}
	return decorator.ApplyCommandDecorators[CreateDriverProfile, CreateDriverProfileResult](handler, logger, metricsClient)
}

func (h *CreateDriverProfileHandler) Handle(ctx context.Context, cmd CreateDriverProfile) (CreateDriverProfileResult, error) {
	profile, err := domain.NewDriverProfile(cmd.UserID, cmd.DriverName, cmd.Phone, cmd.VehicleType, cmd.LicencePlate)
	if err != nil {
		return CreateDriverProfileResult{}, err
	}

	id, err := h.repo.CreateDriverProfile(ctx, profile)
	if err != nil {
		return CreateDriverProfileResult{}, err
	}

	return CreateDriverProfileResult{ID: id}, nil
}
