package bootstrap

import (
	"log/slog"
	"os"
)

func NewLogger(env string) *slog.Logger {
	level := slog.LevelInfo
	if env == "local" || env == "dev" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
