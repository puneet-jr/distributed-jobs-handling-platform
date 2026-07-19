package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	appjob "github.com/your-org/distributed-job-platform/internal/application/job"
	domainjob "github.com/your-org/distributed-job-platform/internal/domain/job"
	httpapi "github.com/your-org/distributed-job-platform/internal/interfaces/http"
	"github.com/your-org/distributed-job-platform/internal/infrastructure/mongo"
	"github.com/your-org/distributed-job-platform/internal/infrastructure/observability"
)

type App struct {
	cfg Config
	server *http.Server
	logger *slog.Logger
}

func NewApp(ctx context.Context, configPath string)(*App, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	logger := NewLogger(cfg.App.Env)

	client, err := mongo.NewClient(cfg,cfg.MONGO_URI)

	if err != nil {
		return nil, err
	}
}

// Observability part complete it understand it first and then complete it

