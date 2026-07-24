package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// Why context with signal handling?
	// Graceful shutdown: when SIGTERM/SIGINT received,
	// context cancels, app stops accepting new requests,
	// finishes in-flight requests, closes DB connections.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Docker sends SIGTERM before killing container.
	// Kubernetes sends SIGTERM before pod deletion.
	// Ctrl+C sends SIGINT during development.
	// Handle them to shut down cleanly.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func(){
		sig := <-sigChan
		logPrintf("received the signal %v initiating shutdown", sig)
	}()

	// Flexibility: dev uses configs/local.yaml, prod uses /etc/app/config.yaml
		// Don't hardcode paths. Make it configurable.
		configPath := os.Getenv("CONFIG_PATH")
		if configPath == "" {
			configPath = "configs/local.yaml"
		}
	
		app, err := bootstrap.NewApp(ctx, configPath)
		if err != nil {
			log.Fatalf("failed to initialize app: %v", err)
		}
		defer app.Close()
	
		if err := app.Run(ctx); err != nil {
			log.Fatalf("server error: %v", err)
		}
	
		log.Println("server stopped")
	}
}
