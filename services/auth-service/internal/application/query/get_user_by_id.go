package query

import (
	"context"

	"auth-service/internal/common/decorator"
	"auth-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetUserByID struct {
	ID string
}

type GetUserByIDHandler struct {
	repo domain.UserRepository
}

func NewGetUserByIDHandler(
	repo domain.UserRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetUserByID, *domain.User] {

	handler := &GetUserByIDHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetUserByID, *domain.User](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetUserByIDHandler) Handle(ctx context.Context, q GetUserByID) (*domain.User, error) {
	return h.repo.GetByID(ctx, q.ID)
}
