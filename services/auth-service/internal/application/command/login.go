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
	clientRepo  domain.ClientRepository
	refreshRepo domain.RefreshTokenRepository
	hasher      services.PasswordHasher
	tokenIssuer services.TokenIssuer
	logger      *logrus.Entry
}

func NewLoginHandler(
	repo domain.UserRepository,
	clientRepo domain.ClientRepository,
	refreshRepo domain.RefreshTokenRepository,
	hasher services.PasswordHasher,
	tokenIssuer services.TokenIssuer,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Login, LoginResult] {

	handler := &LoginHandler{repo: repo, clientRepo: clientRepo, refreshRepo: refreshRepo, hasher: hasher, tokenIssuer: tokenIssuer, logger: logger}

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

	clientID, err := h.lookupClientID(ctx, user)
	if err != nil {
		return LoginResult{}, err
	}

	accessToken, expiresIn, err := h.tokenIssuer.IssueAccess(user.ID, user.Email, user.Role, clientID)
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

// lookupClientID resolves the client row's id for the client_id JWT claim.
// Non-Client roles (and, defensively, a Client with no row yet) mint a
// token with no client_id claim rather than failing login outright.
func (h *LoginHandler) lookupClientID(ctx context.Context, user *domain.User) (string, error) {
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
