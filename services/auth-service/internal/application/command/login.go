package command

import (
	"context"
	"errors"

	"auth-service/internal/application/services"
	"auth-service/internal/common/decorator"
	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"

	"github.com/sirupsen/logrus"
)

type Login struct {
	Email    string
	Password string
}

type LoginResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int
}

type LoginHandler struct {
	repo        domain.UserRepository
	refreshRepo domain.RefreshTokenRepository
	hasher      services.PasswordHasher
	tokenIssuer services.TokenIssuer
	logger      *logrus.Entry
}

func NewLoginHandler(
	repo domain.UserRepository,
	refreshRepo domain.RefreshTokenRepository,
	hasher services.PasswordHasher,
	tokenIssuer services.TokenIssuer,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Login, LoginResult] {

	handler := &LoginHandler{repo: repo, refreshRepo: refreshRepo, hasher: hasher, tokenIssuer: tokenIssuer, logger: logger}

	return decorator.ApplyCommandDecorators[Login, LoginResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *LoginHandler) Handle(ctx context.Context, cmd Login) (LoginResult, error) {
	user, err := h.repo.GetByEmail(ctx, cmd.Email)
	if errors.Is(err, commonerrors.ErrNotFound) {
		return LoginResult{}, commonerrors.ErrInvalidCredentials
	}
	if err != nil {
		return LoginResult{}, err
	}

	if err := h.hasher.Compare(user.PasswordHash, cmd.Password); err != nil {
		return LoginResult{}, commonerrors.ErrInvalidCredentials
	}

	accessToken, expiresIn, err := h.tokenIssuer.IssueAccess(user.ID, user.Email, user.Role)
	if err != nil {
		return LoginResult{}, err
	}
	refreshToken, expiresAt, err := h.tokenIssuer.IssueRefresh(user.ID)
	if err != nil {
		return LoginResult{}, err
	}

	// Store failure is logged and not fatal to the login itself, matching
	// the pre-refactor handler's behavior.
	if err := h.refreshRepo.Store(ctx, &domain.RefreshToken{UserID: user.ID, Token: refreshToken, ExpiresAt: expiresAt}); err != nil {
		h.logger.WithError(err).Warn("failed to store refresh token")
	}

	return LoginResult{AccessToken: accessToken, RefreshToken: refreshToken, ExpiresIn: expiresIn}, nil
}
