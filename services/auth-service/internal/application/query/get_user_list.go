package query

import (
	"context"

	"auth-service/internal/common/decorator"
	"auth-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type GetUserList struct {
	Page     int
	PageSize int
	SortBy   string
	SortDir  string
}

type GetUserListHandler struct {
	repo domain.UserRepository
}

func NewGetUserListHandler(
	repo domain.UserRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.QueryHandler[GetUserList, PagedResult[*domain.User]] {

	handler := &GetUserListHandler{repo: repo}

	return decorator.ApplyQueryDecorators[GetUserList, PagedResult[*domain.User]](
		handler,
		logger,
		metricsClient,
	)
}

func (h *GetUserListHandler) Handle(ctx context.Context, q GetUserList) (PagedResult[*domain.User], error) {
	total, err := h.repo.CountUsers(ctx)
	if err != nil {
		return PagedResult[*domain.User]{}, err
	}

	items, err := h.repo.GetUserList(ctx, domain.PageRequest{
		Page:     q.Page,
		PageSize: q.PageSize,
		SortBy:   q.SortBy,
		SortDir:  q.SortDir,
	})
	if err != nil {
		return PagedResult[*domain.User]{}, err
	}

	return PagedResult[*domain.User]{Items: items, TotalCount: total}, nil
}
