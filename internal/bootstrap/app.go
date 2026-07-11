package bootstrap

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/your-org/distributed-job-platform/internal/infrastructure/mongo"
)

type App struct {
	cfg    Config
	server *http.Server
	logger *slog.Logger
}

func NewApp(ctx context.Context, configPath string) (*App, error) {
	cfg, err := LoadConfig(configPath)

	if err != nil {
		return nil, err
	}

	logger := NewLogger(cfg.App.Env)

	client, err := mongo.NewClient(ctx, cfg.Mongo.URI)

	if err != nil {
		return nil, err
	}

	coll := client.Database(cfg.Mongo.Database).Collection(cfg.Mongo.JobsCollection)
}

	return &App{
		cfg:    cfg,
		server: nil,
		logger: logger,
	},
}


