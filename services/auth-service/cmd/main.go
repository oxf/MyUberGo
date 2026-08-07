package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	app "auth-service/internal/application"
	"auth-service/internal/application/command"
	"auth-service/internal/application/query"
	"auth-service/internal/infrastructure/health"
	"auth-service/internal/infrastructure/metrics"
	"auth-service/internal/infrastructure/security"
	"auth-service/internal/infrastructure/shutdown"
	"auth-service/internal/interfaces/http/handler"
	"auth-service/internal/persistence"

	httpmw "github.com/oxf/MyUber/common/httpmiddleware"
	"github.com/oxf/MyUber/observability/obsdb"
	"github.com/oxf/MyUber/observability/obshttp"
	"github.com/oxf/MyUber/observability/obslog"
	"github.com/oxf/MyUber/observability/otelinit"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

const serviceName = "auth-service"

const (
	defaultPgDsn     = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
	defaultJwtSecret = "secret_change_me"
)

func main() {
	// `app healthcheck` backs Docker's HEALTHCHECK: distroless has no
	// shell/curl, so the binary probes its own /health/ready instead.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheckSelf("http://localhost:8000/health/live")
	}

	// otelinit.Setup installs the global tracer/meter/logger from standard
	// OTEL_* env vars; a down Collector never fails boot (lazy dial+retry).
	ctx := context.Background()
	providers, err := otelinit.Setup(ctx, serviceName)
	if err != nil {
		log.Fatal(err)
	}

	logger := obslog.NewLogger(serviceName)

	dsn := getenv("PG_DSN", defaultPgDsn)
	jwtSecretStr := getenv("JWT_SECRET", defaultJwtSecret)
	if os.Getenv("APP_ENV") == "production" {
		if dsn == defaultPgDsn {
			log.Fatal("refusing to start in production with the default PG_DSN — set a real PG_DSN")
		}
		if jwtSecretStr == defaultJwtSecret {
			log.Fatal("refusing to start in production with the default JWT_SECRET — set a real JWT_SECRET")
		}
	}
	db, err := obsdb.Open("postgres", dsn, "postgresql")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(atoi(getenv("DB_MAX_OPEN_CONNS", "20")))
	db.SetMaxIdleConns(atoi(getenv("DB_MAX_IDLE_CONNS", "10")))
	db.SetConnMaxLifetime(time.Duration(atoi(getenv("DB_CONN_MAX_LIFETIME_MIN", "5"))) * time.Minute)

	jwtSecret := []byte(jwtSecretStr)
	accessTTL := time.Duration(atoi(getenv("AUTH_TOKEN_EXP_MIN", "15"))) * time.Minute
	refreshTTL := time.Duration(atoi(getenv("REFRESH_TOKEN_EXP_HOUR", "24"))) * time.Hour

	userRepo := persistence.NewPostgresUserRepository(db)
	clientRepo := persistence.NewPostgresClientRepository(db)
	refreshTokenRepo := persistence.NewPostgresRefreshTokenRepository(db)
	transactionManager := persistence.NewPostgresTransactionManager(db)
	hasher := security.NewBcryptHasher()
	tokenIssuer := security.NewJWTIssuer(jwtSecret, accessTTL, refreshTTL)

	metricsClient := metrics.NewOtelMetricsClient(serviceName)

	application := app.Application{
		Commands: app.Commands{
			Signup:  command.NewSignupHandler(userRepo, clientRepo, hasher, transactionManager, logger, metricsClient),
			Login:   command.NewLoginHandler(userRepo, clientRepo, refreshTokenRepo, hasher, tokenIssuer, logger, metricsClient),
			Refresh: command.NewRefreshHandler(userRepo, clientRepo, refreshTokenRepo, tokenIssuer, logger, metricsClient),
			Logout:  command.NewLogoutHandler(refreshTokenRepo, logger, metricsClient),
		},
		Queries: app.Queries{
			GetUserList: query.NewGetUserListHandler(userRepo, logger, metricsClient),
			GetUserByID: query.NewGetUserByIDHandler(userRepo, clientRepo, logger, metricsClient),
		},
	}

	authHandler := handler.NewAuthHandler(application, logger)
	userHandler := handler.NewUserHandler(application, logger)

	// Initialize health checker
	healthChecker := health.NewChecker(db, 5*time.Second)
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Health check endpoints
	mux.HandleFunc("GET /health/live", healthChecker.LiveHandler)
	mux.HandleFunc("GET /health/ready", healthChecker.ReadyHandler)

	// API endpoints
	mux.HandleFunc("POST /signup", authHandler.Signup)
	mux.HandleFunc("POST /login", authHandler.Login)
	mux.HandleFunc("POST /refresh", authHandler.Refresh)
	mux.HandleFunc("POST /logout", authHandler.Logout)
	mux.HandleFunc("GET /users", userHandler.GetList)
	mux.HandleFunc("GET /me", userHandler.Me)

	// Create HTTP server
	server := &http.Server{
		Addr:         ":8000",
		Handler:      httpmw.BodyLimit(httpmw.RequestID(obshttp.Handler(httpmw.Recover(logger)(mux), serviceName))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Flip readiness the moment shutdown begins, not up to checkInterval
	// later — the ticker-based DB-ping check alone wouldn't catch it promptly.
	shutdownManager.OnStop(healthChecker.MarkNotReady)

	// Flush providers only after every worker has actually drained, not merely
	// told to stop — an OnStop hook here would silently drop the drain period's telemetry.
	shutdownManager.OnDrained(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			log.Printf("observability shutdown error: %v\n", err)
		}
	})

	// Start server in a goroutine
	goSafe(logger, healthChecker, nil, "http-server", func() {
		logger.Info("auth-service listening on :8000")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("server error")
		}
	})

	// Wait for shutdown signal and perform graceful shutdown
	shutdownManager.WaitForShutdown()
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func atoi(s string) int { v, _ := strconv.Atoi(s); return v }

// healthcheckSelf backs the `app healthcheck` subcommand: a plain HTTP GET
// against the service's own readiness endpoint, exiting 0/1 for Docker.
func healthcheckSelf(url string) {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}

// goSafe runs fn in a goroutine, recovering panics. If workerCtx is non-nil
// and fn returns before it's cancelled, that's a genuine liveness failure.
func goSafe(logger *logrus.Entry, healthChecker *health.Checker, workerCtx context.Context, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("goroutine", name).WithField("panic", r).Error("recovered from panic")
			}
			if workerCtx != nil && workerCtx.Err() == nil {
				healthChecker.MarkNotLive(name + " exited unexpectedly")
			}
		}()
		fn()
	}()
}
