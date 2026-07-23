package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	appjob "github.com/your-org/distributed-job-platform/internal/application/job"
	httpapi "github.com/your-org/distributed-job-platform/internal/interfaces/http"
)

type App struct {
	cfg    *Config
	server *http.Server
	logger *slog.Logger
	db     *sql.DB
}

func NewApp(ctx context.Context, configPath string) (*App, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	// Step 2: Initialize logger
	// Why slog? It's Go's standard structured logging (since 1.21).
	// Better than fmt.Printf: includes levels, context, JSON output.
	logger := NewLogger(cfg.App.Env)
	logger.Info("starting application bootstrap", "env", cfg.App.Env)

	db, err := sql.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Why PingContext?
	// sql.Open is lazy, so we must verify connection works NOW.
	// Otherwise, first HTTP request fails with "database not connected".
	// Fail fast on startup.
	if err := db.PingContext(ctx); err != nil {
		db.Close() // Why close? Clean up resources before returning error.
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Connection Max Life time?
	// Postgres has statement cache. If connection lives forever,
	// cache grows unbounded. Periodic recreation keeps cache fresh.
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(25) // Why 25? Reasonable default. Tune based on load.
	db.SetMaxIdleConns(5)  // Why 5? Keep some warm connections, not all.

	logger.Info("database connection established")

	// Create repository

	repo, err := postgres.NewJobRepository(db)

	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create a repository: %w",err)
	}

	// Create application service
	// Why inject repository?
	// Dependency Injection. Service doesn't create repo, it receives it.
	// This makes testing easy: pass mock repository instead of real Postgres.
	jobService := appjob.NewService(repo)

	//Create HTTP handler

	jobHandler := httpapi.NewJobHandler(jobService)

	// Create router
	router := httpapi.NewRouter(
		jobHandler,
		NewHealthHandler(logger, db), // Why pass db? Health check needs to verify DB is alive.
	) 

	// Step 8: Configure HTTP server
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,  // Why? Prevent Slowloris attacks
		ReadTimeout:       30 * time.Second, // Why? Limit request body read time
		WriteTimeout:      30 * time.Second, // Why? Limit response write time
		IdleTimeout:       60 * time.Second, // Why? Close idle connections
	}

	logger.Info("database connection established")

return &App{
	cfg:  cfg,
	server: server,
	logger: logger,
	db: db,
}, nil
}

func(a *App) Run(ctx context.Context) error {
	a.logger.Info("starting api server", "port",a.cfg.HTTP.Port)

	// Why goroutine for shutdown?
	// ListenAndServe blocks. We need to listen for ctx.Done() simultaneously.
	// Goroutine allows concurrent shutdown signal handling.
	go func() {
		<-ctx.Done()
		a.logger.Info("shutting down server...")
		
		// Why Shutdown with timeout?
		// Graceful shutdown: finish in-flight requests, but don't wait forever.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := a.server.Shutdown(shutdownCtx); err != nil {
			a.logger.Error("server shutdown failed","error",err)
		}
	}()

	if err := a.server.ListenAndServer(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("save error: %w",err)
	}
	return nil
}


func (a *App) Close() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}
// Alternative: use "github.com/jackc/pgx/v5/stdlib" for better performance.
// 