package command

import (
	"context"

	"auth-service/internal/application/services"
	"auth-service/internal/common/decorator"
	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"

	"github.com/sirupsen/logrus"
)

type Signup struct {
	Email    string
	Password string
	Name     string
	Phone    string
	Role     contracts.UserRole
}

type SignupResult struct {
	UserID string
}

type SignupHandler struct {
	repo   domain.UserRepository
	hasher services.PasswordHasher
}

func NewSignupHandler(
	repo domain.UserRepository,
	hasher services.PasswordHasher,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Signup, SignupResult] {

	handler := &SignupHandler{repo: repo, hasher: hasher}

	return decorator.ApplyCommandDecorators[Signup, SignupResult](
		handler,
		logger,
		metricsClient,
	)
}

func (h *SignupHandler) Handle(ctx context.Context, cmd Signup) (SignupResult, error) {
	hash, err := h.hasher.Hash(cmd.Password)
	if err != nil {
		return SignupResult{}, err
	}

	user, err := domain.NewUser(cmd.Email, hash, cmd.Name, cmd.Phone, cmd.Role)
	if err != nil {
		return SignupResult{}, err
	}

	id, err := h.repo.CreateUser(ctx, user)
	if err != nil {
		return SignupResult{}, err
	}

	return SignupResult{UserID: id}, nil
}
