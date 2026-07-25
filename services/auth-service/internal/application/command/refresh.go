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

type Refresh struct {
	RefreshToken string
}

type RefreshResult struct {
	AccessToken string
	ExpiresIn   int
}

type RefreshHandler struct {
	repo        domain.UserRepository
	refreshRepo domain.RefreshTokenRepository
	tokenIssuer services.TokenIssuer
}

func NewRefreshHandler(
	repo domain.UserRepository,
	refreshRepo domain.RefreshTokenRepository,
	tokenIssuer services.TokenIssuer,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Refresh, RefreshResult] {

	handler := &RefreshHandler{repo: repo, refreshRepo: refreshRepo, tokenIssuer: tokenIssuer}

	return decorator.ApplyCommandDecorators[Refresh, RefreshResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *RefreshHandler) Handle(ctx context.Context, cmd Refresh) (RefreshResult, error) {
	uid, err := h.tokenIssuer.ParseRefresh(cmd.RefreshToken)
	if err != nil {
		return RefreshResult{}, commonerrors.ErrInvalidToken
	}

	exists, err := h.refreshRepo.Exists(ctx, uid, cmd.RefreshToken)
	if err != nil {
		return RefreshResult{}, err
	}
	if !exists {
		return RefreshResult{}, commonerrors.ErrInvalidToken
	}

	// Re-fetch email/role so a stale/rotated profile doesn't get baked into
	// a fresh access token.
	user, err := h.repo.GetByID(ctx, uid)
	if errors.Is(err, commonerrors.ErrNotFound) {
		return RefreshResult{}, commonerrors.ErrInvalidToken
	}
	if err != nil {
		return RefreshResult{}, err
	}

	accessToken, expiresIn, err := h.tokenIssuer.IssueAccess(user.ID, user.Email, user.Role)
	if err != nil {
		return RefreshResult{}, err
	}

	return RefreshResult{AccessToken: accessToken, ExpiresIn: expiresIn}, nil
}
