package command

import (
	"context"
	"errors"

	"auth-service/internal/application/services"
	"auth-service/internal/common/decorator"
	"auth-service/internal/domain"

	contracts "github.com/oxf/MyUber/contracts/http"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
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
	metrics     decorator.MetricsClient
}

func NewSignupHandler(
	repo domain.UserRepository,
	clientRepo domain.ClientRepository,
	hasher services.PasswordHasher,
	transaction services.TransactionManager,
	logger *logrus.Entry,
	metricsClient decorator.MetricsClient,
) decorator.CommandHandler[Signup, SignupResult] {

	handler := &SignupHandler{repo: repo, clientRepo: clientRepo, hasher: hasher, transaction: transaction, metrics: metricsClient}

	return decorator.ApplyCommandDecorators[Signup, SignupResult](
		handler,
		logger,
		metricsClient,
	)
}

// minPasswordLength is intentionally a plain length floor, not a full
// complexity policy (upper/lower/digit/symbol classes) — a length minimum
// blocks the worst offenders (empty, "1234") without the UX cost of a rule
// set that rejects strong-but-unusual passwords.
const minPasswordLength = 8

func (h *SignupHandler) Handle(ctx context.Context, cmd Signup) (SignupResult, error) {
	if len(cmd.Password) < minPasswordLength {
		return SignupResult{}, errors.New("password must be at least 8 characters")
	}

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

	if err == nil && h.metrics != nil {
		h.metrics.IncCounter(ctx, "myubergo.signups", attribute.String("role", string(cmd.Role)))
	}

	return result, err
}
