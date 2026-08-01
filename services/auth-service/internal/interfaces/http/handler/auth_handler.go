package handler

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	app "auth-service/internal/application"
	"auth-service/internal/application/command"
	commonerrors "auth-service/internal/common/errors"

	contracts "github.com/oxf/MyUber/contracts/http"
)

type AuthHandler struct {
	app app.Application
}

func NewAuthHandler(app app.Application) *AuthHandler {
	return &AuthHandler{app: app}
}

func (h *AuthHandler) Signup(w http.ResponseWriter, r *http.Request) {
	var req contracts.SignupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Email == "" || req.Password == "" {
		writeError(w, "invalid credentials", http.StatusBadRequest)
		return
	}
	if req.Role != contracts.RoleClient && req.Role != contracts.RoleDriver {
		// Admin has no signup path — the one seeded admin account is
		// inserted directly by services/shared/migrations/init.sql (see
		// CLAUDE.md's "Data model" section).
		writeError(w, "role must be Client or Driver", http.StatusBadRequest)
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
		writeError(w, err.Error(), http.StatusConflict)
		return
	case err != nil:
		writeInternalError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	writeJSON(w, contracts.SignupResponse{UserID: result.UserID})
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req contracts.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.Login.Handle(r.Context(), command.Login{
		Email:    req.Email,
		Password: req.Password,
	})
	switch {
	case errors.Is(err, commonerrors.ErrInvalidCredentials):
		writeError(w, "invalid credentials", http.StatusUnauthorized)
		return
	case err != nil:
		writeInternalError(w, err)
		return
	}

	writeJSON(w, contracts.LoginResponse{
		AccessToken:  result.AccessToken,
		RefreshToken: result.RefreshToken,
		ExpiresIn:    result.ExpiresIn,
	})
}

func (h *AuthHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	var req contracts.RefreshRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	result, err := h.app.Commands.Refresh.Handle(r.Context(), command.Refresh{
		RefreshToken: req.RefreshToken,
	})
	switch {
	case errors.Is(err, commonerrors.ErrInvalidToken):
		writeError(w, "invalid or expired refresh token", http.StatusUnauthorized)
		return
	case err != nil:
		writeInternalError(w, err)
		return
	}

	writeJSON(w, contracts.RefreshResponse{
		AccessToken: result.AccessToken,
		ExpiresIn:   result.ExpiresIn,
	})
}

func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-Id")
	if userID == "" {
		writeError(w, "X-User-Id header is required", http.StatusBadRequest)
		return
	}

	var req contracts.LogoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.app.Commands.Logout.Handle(r.Context(), command.Logout{
		UserID:       userID,
		RefreshToken: req.RefreshToken,
	}); err != nil {
		writeInternalError(w, err)
		return
	}

	writeJSON(w, map[string]bool{"ok": true})
}

func writeError(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

// writeInternalError logs the real error server-side and returns a generic
// message to the client — the raw error text (which can include SQL/driver
// internals) must never reach an HTTP response.
func writeInternalError(w http.ResponseWriter, err error) {
	log.Println("internal error:", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
