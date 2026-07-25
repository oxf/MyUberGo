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

func main() {
	dsn := getenv("PG_DSN", "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable")
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}

	jwtSecret := []byte(getenv("JWT_SECRET", "secret_change_me"))
	accessTTL := time.Duration(atoi(getenv("AUTH_TOKEN_EXP_MIN", "15"))) * time.Minute
	refreshTTL := time.Duration(atoi(getenv("REFRESH_TOKEN_EXP_HOUR", "24"))) * time.Hour

	userRepo := persistence.NewPostgresUserRepository(db)
	refreshTokenRepo := persistence.NewPostgresRefreshTokenRepository(db)
	hasher := security.NewBcryptHasher()
	tokenIssuer := security.NewJWTIssuer(jwtSecret, accessTTL, refreshTTL)

	// create logger and metrics client used by decorators
	logger := logrus.NewEntry(logrus.New())
	metricsClient := metrics.NewLoggingMetricsClient(logger)

	application := app.Application{
		Commands: app.Commands{
			Signup:  command.NewSignupHandler(userRepo, hasher, logger, metricsClient),
			Login:   command.NewLoginHandler(userRepo, refreshTokenRepo, hasher, tokenIssuer, logger, metricsClient),
			Refresh: command.NewRefreshHandler(userRepo, refreshTokenRepo, tokenIssuer, logger, metricsClient),
			Logout:  command.NewLogoutHandler(refreshTokenRepo, logger, metricsClient),
		},
		Queries: app.Queries{
			GetUserList: query.NewGetUserListHandler(userRepo, logger, metricsClient),
			GetUserByID: query.NewGetUserByIDHandler(userRepo, logger, metricsClient),
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
		Handler:      middleware.RequestID(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Create shutdown manager with 30s timeout
	shutdownManager := shutdown.NewManager(server, 30*time.Second)

	// Start server in a goroutine
	go func() {
		log.Println("auth-service listening on :8000")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
		}
	}()

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
