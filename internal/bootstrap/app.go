package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	appjob "distributed-job-platform/internal/application/job"
	"distributed-job-platform/internal/infrastructure/postgres"
	redisqueue "distributed-job-platform/internal/infrastructure/redisqueue"
	httpapi "distributed-job-platform/internal/interfaces/http"
)

type App struct {
	cfg    *Config
	server *http.Server
	logger *slog.Logger
	db     *sql.DB
	redis  *redis.Client
}

func NewApp(ctx context.Context, configPath string) (*App, error) {
	// Step 1: Load configuration
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Step 2: Initialize logger
	logger := NewLogger(cfg.App.Env)
	logger.Info("starting application bootstrap", "env", cfg.App.Env)

	// Step 3: Connect to Database
	db, err := sql.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Why PingContext? Fail fast on startup instead of failing on the first HTTP request.
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Configure connection pool to prevent resource exhaustion
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	logger.Info("database connection established")

	// Step 4: Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	// Why Ping Redis on startup?
	// The API cannot honestly return 202 Accepted if it cannot enqueue jobs.
	if err := redisClient.Ping(ctx).Err(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping redis: %w", err)
	}
	logger.Info("redis connection established")

	// Step 5: Initialize Queue Abstraction
	jobQueue, err := redisqueue.NewJobQueue(redisClient, "jobs")
	if err != nil {
		_ = redisClient.Close()
		_ = db.Close()
		return nil, fmt.Errorf("failed to create job queue: %w", err)
	}

	// Step 6: Create Repository
	repo, err := postgres.NewJobRepository(db)
	if err != nil {
		_ = redisClient.Close()
		_ = db.Close()
		return nil, fmt.Errorf("failed to create repository: %w", err)
	}

	// Step 7: Create Application Service (injecting BOTH repo and queue)
	// This enforces the rule: validate -> store in Postgres -> enqueue in Redis -> return 202
	jobService := appjob.NewService(repo, jobQueue)

	// Step 8: Create HTTP Handler and Router
	jobHandler := httpapi.NewJobHandler(jobService)
	router := httpapi.NewRouter(
		jobHandler,
		NewHealthHandler(logger, db),
	)

	// Step 9: Configure HTTP Server with secure timeouts
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,  // Prevents Slowloris attacks
		ReadTimeout:       30 * time.Second, // Limits request body read time
		WriteTimeout:      30 * time.Second, // Limits response write time
		IdleTimeout:       60 * time.Second, // Closes idle keep-alive connections
	}

	return &App{
		cfg:    cfg,
		server: server,
		logger: logger,
		db:     db,
		redis:  redisClient,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	a.logger.Info("starting api server", "port", a.cfg.HTTP.Port)

	// Goroutine for graceful shutdown
	go func() {
		<-ctx.Done()
		a.logger.Info("shutting down server...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := a.server.Shutdown(shutdownCtx); err != nil {
			a.logger.Error("server shutdown failed", "error", err)
		}
	}()

	// FIXED: ListenAndServe (was ListenAndServer)
	if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		// FIXED: "server error" (was "save error")
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

func (a *App) Close() error {
	var errs []error

	// Close Redis first (it's stateless for the app, just a queue)
	if a.redis != nil {
		if err := a.redis.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close redis: %w", err))
		}
	}

	// Close Database second
	if a.db != nil {
		if err := a.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close database: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}
