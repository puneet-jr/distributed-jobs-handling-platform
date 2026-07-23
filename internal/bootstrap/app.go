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
	cfg 	Config
	server *http.Server
	logger *slog.Logger
	db     *sql.DB
}

func NewApp(ctx context.Context, configPath string) (*App, error) {
	cfg, err:= LoadConfig(configPath)
	if err!= nil {
		return nil, err
	}

	// Step 2: Initialize logger
	// Why slog? It's Go's standard structured logging (since 1.21).
	// Better than fmt.Printf: includes levels, context, JSON output.
	logger := NewLogger(cfg.App.Env)
	logger.Info("starting application bootstrap","env",cfg.App.Env)

	db, err := sql.Open("postgres",cfg.Postgres.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w",err)
	}

	// Why PingContext?
		// sql.Open is lazy, so we must verify connection works NOW.
		// Otherwise, first HTTP request fails with "database not connected".
		// Fail fast on startup.
		if err := db.PingContext(ctx); err != nil {
			db.Close() // Why close? Clean up resources before returning error.
			return nil, fmt.Errorf("failed to ping database: %w", err)
		}
		

}

} 

server = := &http.Server{
	Addr :=		fmt.Sprintf(":%d", cfg.HTTP.Port)
	Handler := router,
	ReadHeaderTimeout := 5 * time.Second,
}

return &App {
	cfg: cfg,
	server: server,
	logger: logger,
},nil
}

func( a *App) Run(ctx context.Context) error {
	a.logger.Info("starting api server","port", a.cfg.HTTP.Port)
	return a.server.ListenAndServe()
}
