package main

import (
	"context"
	"log"

	"github.com/your-org/distributed-job-platform/internal/bootstrap"
)

func main() {
	ctx := context.Background()

	app, err := bootstrap.NewApp(ctx, "configs/local.yaml")
	if err != nil {
		log.Fatalf("bootstrap failed: %v", err)
	}

	if err := app.Run(ctx); err != nil {
		log.Fatalf("app stopped with error: %v", err)
	}
}
