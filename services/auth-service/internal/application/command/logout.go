package command

import (
	"context"

	"auth-service/internal/common/decorator"
	"auth-service/internal/domain"

	"github.com/sirupsen/logrus"
)

// Logout revokes one refresh token, scoped to the caller (UserID comes from
// the gateway-injected X-User-Id, not from the token body itself), so a
// caller can only revoke their own sessions.
type Logout struct {
	UserID       string
	RefreshToken string
}

type LogoutHandler struct {
	refreshRepo domain.RefreshTokenRepository
}

func NewLogoutHandler(
	refreshRepo domain.RefreshTokenRepository,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandlerNoResult[Logout] {

	handler := &LogoutHandler{refreshRepo: refreshRepo}

	return decorator.ApplyCommandDecoratorsNoResult[Logout](
		handler,
		logger,
		metricsClient,
	)
}

func (h *LogoutHandler) Handle(ctx context.Context, cmd Logout) error {
	return h.refreshRepo.Delete(ctx, cmd.UserID, cmd.RefreshToken)
}
