package handler

import (
	"errors"
	"net/http"

	app "auth-service/internal/application"
	"auth-service/internal/application/query"
	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	"github.com/oxf/MyUber/common/paging"
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
	params, err := paging.ParseListParams(r, domain.UserSortColumns, "createdAt")
	if err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Queries.GetUserList.Handle(r.Context(), query.GetUserList{
		Page: params.Page, PageSize: params.PageSize, SortBy: params.SortBy, SortDir: params.SortDir,
	})
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	items := make([]contracts.UserDto, 0, len(result.Items))
	for _, user := range result.Items {
		items = append(items, toUserDto(user))
	}
	httpresponse.WriteJSON(w, http.StatusOK, contracts.PagedResponse[contracts.UserDto]{
		Items: items, Page: params.Page, PageSize: params.PageSize, TotalCount: result.TotalCount,
	})
}

// Me backs GET /me: the caller's own profile, identified by the gateway's
// X-User-Id header, never a client-supplied id.
func (h *UserHandler) Me(w http.ResponseWriter, r *http.Request) {
	userID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}

	result, err := h.app.Queries.GetUserByID.Handle(r.Context(), query.GetUserByID{ID: userID})
	if errors.Is(err, commonerrors.ErrNotFound) {
		httpresponse.WriteError(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, toUserDto(result))
}
