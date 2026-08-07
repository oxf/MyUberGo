package handler

import (
	"errors"
	"net/http"

	app "auth-service/internal/application"
	"auth-service/internal/application/query"
	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type UserHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewUserHandler(app app.Application, logger *logrus.Entry) *UserHandler {
	return &UserHandler{app: app, logger: logger}
}

func (h *UserHandler) GetList(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r, domain.UserSortColumns, "createdAt")
	if err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetUserList.Handle(r.Context(), query.GetUserList{
		Page: params.page, PageSize: params.pageSize, SortBy: params.sortBy, SortDir: params.sortDir,
	})
	if err != nil {
		writeInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.UserDto, 0, len(result.Items))
	for _, user := range result.Items {
		items = append(items, toUserDto(user))
	}
	writeJSON(w, contracts.PagedResponse[contracts.UserDto]{
		Items: items, Page: params.page, PageSize: params.pageSize, TotalCount: result.TotalCount,
	})
}

// Me backs GET /me: the caller's own profile, identified by the gateway's
// X-User-Id header, never a client-supplied id.
func (h *UserHandler) Me(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-Id")
	if userID == "" {
		writeError(w, "X-User-Id header is required", http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetUserByID.Handle(r.Context(), query.GetUserByID{ID: userID})
	if errors.Is(err, commonerrors.ErrNotFound) {
		writeError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeInternalError(w, r, err, h.logger)
		return
	}

	writeJSON(w, toUserDto(result))
}
