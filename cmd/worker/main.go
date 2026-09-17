package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	appjob "distributed-job-platform/internal/application/job"
	"distributed-job-platform/internal/bootstrap"
	"distributed-job-platform/internal/infrastructure/postgres"
	"distributed-job-platform/internal/infrastructure/redisqueue"
	"distributed-job-platform/internal/observability"
	"distributed-job-platform/internal/workers"
)

func main() {
	// Setup context with graceful shutdown handling
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Force immediate exit on second signal
	go func() {
		<-ctx.Done()
		second := make(chan os.Signal, 1)
		signal.Notify(second, syscall.SIGINT, syscall.SIGTERM)
		<-second
		fmt.Fprintln(os.Stderr, "received second signal, forcing immediate exit")
		os.Exit(1)
	}()

	// Run the application and handle exit status cleanly
	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "application failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/local.yaml"
	}

	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logger := bootstrap.NewLogger(cfg.App.Env)

	// ------------------------------------------------------------
	// PostgreSQL
	// ------------------------------------------------------------
	db, err := sql.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		return fmt.Errorf("failed to open postgres: %w", err)
	}
	defer db.Close()

	// Consider moving these to cfg.Postgres.* for environment-specific tuning
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	// ------------------------------------------------------------
	// Redis
	// ------------------------------------------------------------
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address(),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to ping redis: %w", err)
	}

	const stream = "jobs"
	const group = "job-workers"

	if err := redisqueue.EnsureConsumerGroup(ctx, redisClient, stream, group); err != nil {
		return fmt.Errorf("failed to ensure consumer group: %w", err)
	}

	consumer, err := redisqueue.NewJobConsumer(redisClient, stream, group, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to create queue consumer: %w", err)
	}

	producer, err := redisqueue.NewJobQueue(redisClient, stream)
	if err != nil {
		return fmt.Errorf("failed to create queue producer: %w", err)
	}

	// ------------------------------------------------------------
	// PostgreSQL repository
	// ------------------------------------------------------------
	repo, err := postgres.NewJobRepository(db)
	if err != nil {
		return fmt.Errorf("failed to create job repository: %w", err)
	}

	// ------------------------------------------------------------
	// Prometheus metrics
	// ------------------------------------------------------------
	registryMetrics := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registryMetrics)

	metricsAddr := envString("WORKER_METRICS_ADDR", ":9091")

	// Start metrics server in background
	go serveMetrics(ctx, logger, metricsAddr, registryMetrics)

	// ------------------------------------------------------------
	// Job service & Worker setup
	// ------------------------------------------------------------
	jobService, err := appjob.NewService(repo, producer, metrics)
	if err != nil {
		return fmt.Errorf("failed to create job service: %w", err)
	}

	workerID := envString("WORKER_ID", fmt.Sprintf("worker-%s", mustHostname()))

	registry := workers.HandlerRegistry{
		"email.send":   workers.NewEmailHandler(logger),
		"pdf.generate": workers.NewPDFHandler(logger),
	}

	concurrency := envInt("WORKER_CONCURRENCY", 10)
	batchSize := envInt("WORKER_BATCH_SIZE", concurrency)

	worker, err := workers.NewWorker(
		workerID,
		consumer,
		jobService,
		registry,
		logger,
		concurrency,
		batchSize,
		metrics,
	)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}

	logger.Info("worker started",
		"worker_id", workerID,
		"concurrency", concurrency,
		"batch_size", batchSize,
		"metrics_addr", metricsAddr,
	)

	// Block until worker completes or context is canceled
	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("worker stopped with error: %w", err)
	}

	logger.Info("worker stopped gracefully")
	return nil
}

// serveMetrics exposes the worker's Prometheus metrics and health endpoints.
func serveMetrics(ctx context.Context, logger *slog.Logger, addr string, registry *prometheus.Registry) {
	mux := http.NewServeMux()

	// Health check for orchestrators (Kubernetes, etc.)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown for metrics server
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("worker metrics server shutdown failed", "error", err)
		}
	}()

	logger.Info("worker metrics server started", "addr", addr)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("worker metrics server failed", "error", err)
	}
}

// --- Helper Functions ---

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}