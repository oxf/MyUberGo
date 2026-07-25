package command

import (
	"context"
	"errors"

	"auth-service/internal/application/services"
	"auth-service/internal/common/decorator"
	commonerrors "auth-service/internal/common/errors"
	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"

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
	clientRepo  domain.ClientRepository
	refreshRepo domain.RefreshTokenRepository
	tokenIssuer services.TokenIssuer
	logger      *logrus.Entry
}

func NewRefreshHandler(
	repo domain.UserRepository,
	clientRepo domain.ClientRepository,
	refreshRepo domain.RefreshTokenRepository,
	tokenIssuer services.TokenIssuer,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Refresh, RefreshResult] {

	handler := &RefreshHandler{repo: repo, clientRepo: clientRepo, refreshRepo: refreshRepo, tokenIssuer: tokenIssuer, logger: logger}

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

	clientID, err := h.lookupClientID(ctx, user)
	if err != nil {
		return RefreshResult{}, err
	}

	accessToken, expiresIn, err := h.tokenIssuer.IssueAccess(user.ID, user.Email, user.Role, clientID)
	if err != nil {
		return RefreshResult{}, err
	}

	return RefreshResult{AccessToken: accessToken, ExpiresIn: expiresIn}, nil
}

// lookupClientID resolves the client row's id for the client_id JWT claim.
// Non-Client roles (and, defensively, a Client with no row yet) mint a
// token with no client_id claim rather than failing the refresh outright.
func (h *RefreshHandler) lookupClientID(ctx context.Context, user *domain.User) (string, error) {
	if user.Role != string(contracts.RoleClient) {
		return "", nil
	}
	client, err := h.clientRepo.GetByUserID(ctx, user.ID)
	if errors.Is(err, commonerrors.ErrNotFound) {
		h.logger.Warnf("client %s has no auth.client row", user.ID)
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return client.ID, nil
}
