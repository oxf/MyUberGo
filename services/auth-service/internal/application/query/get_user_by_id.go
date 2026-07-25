package query

import (
	"context"
	"errors"

	"auth-service/internal/common/decorator"
	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"

	"github.com/sirupsen/logrus"
)

type GetUserByID struct {
	ID string
}

type GetUserByIDHandler struct {
	repo       domain.UserRepository
	clientRepo domain.ClientRepository
}

func NewGetUserByIDHandler(
	repo domain.UserRepository,
	clientRepo domain.ClientRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetUserByID, *domain.User] {

	handler := &GetUserByIDHandler{repo: repo, clientRepo: clientRepo}

	return decorator.ApplyQueryDecorators[GetUserByID, *domain.User](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetUserByIDHandler) Handle(ctx context.Context, q GetUserByID) (*domain.User, error) {
	user, err := h.repo.GetByID(ctx, q.ID)
	if err != nil {
		return nil, err
	}

	if user.Role == string(contracts.RoleClient) {
		client, err := h.clientRepo.GetByUserID(ctx, user.ID)
		if err == nil {
			user.ClientID = &client.ID
		} else if !errors.Is(err, commonerrors.ErrNotFound) {
			return nil, err
		}
	}

	return user, nil
}
