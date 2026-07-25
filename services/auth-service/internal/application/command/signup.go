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
	repo        domain.UserRepository
	clientRepo  domain.ClientRepository
	hasher      services.PasswordHasher
	transaction services.TransactionManager
}

func NewSignupHandler(
	repo domain.UserRepository,
	clientRepo domain.ClientRepository,
	hasher services.PasswordHasher,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Signup, SignupResult] {

	handler := &SignupHandler{repo: repo, clientRepo: clientRepo, hasher: hasher, transaction: transaction}

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

	var result SignupResult
	err = h.transaction.WithinTransaction(ctx, func(ctx context.Context) error {
		id, err := h.repo.CreateUser(ctx, user)
		if err != nil {
			return err
		}

		// Driver rows are deliberately not created here — driver.driver lives
		// in driver-service's schema, so it's created via POST /api/driver/driver
		// (see CLAUDE.md "API Gateway" / role-table refactor notes).
		if cmd.Role == contracts.RoleClient {
			if _, err := h.clientRepo.Create(ctx, &domain.Client{UserID: id}); err != nil {
				return err
			}
		}

		result = SignupResult{UserID: id}
		return nil
	})

	return result, err
}
