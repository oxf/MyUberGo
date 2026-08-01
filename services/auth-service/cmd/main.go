package main

import (
	"database/sql"
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
	"auth-service/internal/interfaces/http/middleware"
	"auth-service/internal/persistence"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

const (
	defaultPgDsn     = "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
	defaultJwtSecret = "secret_change_me"
)

func main() {
	// `app healthcheck` is invoked by Docker's HEALTHCHECK (see
	// docker-compose.yml) — distroless has no shell/curl for a CMD-SHELL
	// check, so the binary probes its own /health/ready and exits 0/1.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthcheckSelf("http://localhost:8000/health/ready")
	}

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
	db, err := sql.Open("postgres", dsn)
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

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

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

	authHandler := handler.NewAuthHandler(application)
	userHandler := handler.NewUserHandler(application)

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
		Handler:      middleware.BodyLimit(middleware.RequestID(middleware.Recover(logger)(mux))),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Start server in a goroutine
	goSafe(logger, "http-server", func() {
		log.Println("auth-service listening on :8000")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
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

// goSafe launches fn in a goroutine, recovering any panic so one bad
// message/query doesn't take the whole process down silently.
func goSafe(logger *logrus.Entry, name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.WithField("goroutine", name).WithField("panic", r).Error("recovered from panic")
			}
		}()
		fn()
	}()
}
