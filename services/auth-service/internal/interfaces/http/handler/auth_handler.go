package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	app "auth-service/internal/application"
	"auth-service/internal/application/command"
	commonerrors "auth-service/internal/common/errors"

	"github.com/oxf/MyUber/common/httpresponse"
	"github.com/oxf/MyUber/common/kongheaders"
	contracts "github.com/oxf/MyUber/contracts/http"
	"github.com/sirupsen/logrus"
)

type AuthHandler struct {
	app    app.Application
	logger *logrus.Entry
}

func NewAuthHandler(app app.Application, logger *logrus.Entry) *AuthHandler {
	return &AuthHandler{app: app, logger: logger}
}

func (h *AuthHandler) Signup(w http.ResponseWriter, r *http.Request) {
	var req contracts.SignupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Email == "" || req.Password == "" {
		httpresponse.WriteError(w, "invalid credentials", http.StatusBadRequest)
		return
	}
	if req.Role != contracts.RoleClient && req.Role != contracts.RoleDriver {
		// Admin has no signup path; the one seeded admin account is inserted
		// directly by services/shared/migrations/sql/0002_auth.up.sql.
		httpresponse.WriteError(w, "role must be Client or Driver", http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.Signup.Handle(r.Context(), command.Signup{
		Email:    req.Email,
		Password: req.Password,
		Name:     req.Name,
		Phone:    req.Phone,
		Role:     req.Role,
	})
	switch {
	case errors.Is(err, commonerrors.ErrConflict):
		httpresponse.WriteError(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusCreated, contracts.SignupResponse{UserID: result.UserID})
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req contracts.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.Login.Handle(r.Context(), command.Login{
		Email:    req.Email,
		Password: req.Password,
	})
	switch {
	case errors.Is(err, commonerrors.ErrInvalidCredentials):
		httpresponse.WriteError(w, "invalid credentials", http.StatusUnauthorized)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.LoginResponse{
		AccessToken:  result.AccessToken,
		RefreshToken: result.RefreshToken,
		ExpiresIn:    result.ExpiresIn,
	})
}

func (h *AuthHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	var req contracts.RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.Refresh.Handle(r.Context(), command.Refresh{
		RefreshToken: req.RefreshToken,
	})
	switch {
	case errors.Is(err, commonerrors.ErrInvalidToken):
		httpresponse.WriteError(w, "invalid or expired refresh token", http.StatusUnauthorized)
		return
	case err != nil:
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, contracts.RefreshResponse{
		AccessToken: result.AccessToken,
		ExpiresIn:   result.ExpiresIn,
	})
}

func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	userID, ok := kongheaders.RequireUserID(w, r)
	if !ok {
		return
	}

	var req contracts.LogoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpresponse.WriteError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.app.Commands.Logout.Handle(r.Context(), command.Logout{
		UserID:       userID,
		RefreshToken: req.RefreshToken,
	}); err != nil {
		httpresponse.WriteInternalError(w, r, err, h.logger)
		return
	}

	httpresponse.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
