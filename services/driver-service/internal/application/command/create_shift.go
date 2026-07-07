package command

import (
	"context"
	"driver-service/internal/domain"
	"errors"
)

type CreateShift struct {
	DriverID string
}

type CreateShiftResult struct {
	ID string
}

type CreateShiftHandler struct {
	repo domain.ShiftRepository
}

func NewCreateShiftHandler(repo domain.ShiftRepository) *CreateShiftHandler {
	return &CreateShiftHandler{repo: repo}
}

func (h *CreateShiftHandler) Handle(ctx context.Context, cmd CreateShift) (CreateShiftResult, error) {
	if cmd.DriverID == "" {
		return CreateShiftResult{}, errors.New("driverID is required")
	}

	exists, err := h.repo.HasActiveShift(ctx, cmd.DriverID)
	if err != nil {
		return CreateShiftResult{}, err
	}
	if exists {
		return CreateShiftResult{}, errors.New("active shift already exists")
	}

	shift, err := domain.NewShift(cmd.DriverID)
	if err != nil {
		return CreateShiftResult{}, err
	}

	id, err := h.repo.CreateShift(ctx, shift)
	if err != nil {
		return CreateShiftResult{}, err
	}

	return CreateShiftResult{ID: id}, nil
}
