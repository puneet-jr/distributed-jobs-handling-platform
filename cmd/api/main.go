package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-job-platform/internal/bootstrap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/local.yaml"
	}

	app, err := bootstrap.NewApp(ctx, configPath)
	if err != nil {
		log.Fatalf("bootstrap failed: %v", err)
	}
	defer app.Close()

	if err := app.Run(ctx); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
