package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

appjob "github.com/your-org/distributed-job-platform/internal/application/job"
httpapi "github.com/your-org/distributed-job-platform/internal/interfaces/http"

type App struct {
	cfg 	Config
	server *http.Server
	logger *slog.Logger
}

func NewApp(ctx context.Context, configPath string) (*App, error) {
	cfg, err:= LoadConfig(configPath)
	if err!= nil {
		return nil, err
	}

	logger := NewLogger(cfg.App.Env)

	repo, err := NewPostgresJobRepository(ctx,cfg)

	if err != nil {
		return nil, err
	}

	jobService	:= appjob.NewService(repo)
	jobHandler := httpapi.NewHandler(jobService)
	router := httpapi.NewRouter(jobHandler, func(w http.ResponseWriter, r *http.Request)) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
})

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
