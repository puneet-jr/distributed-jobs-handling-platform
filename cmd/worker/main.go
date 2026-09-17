package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	appjob "distributed-job-platform/internal/application/job"
	"distributed-job-platform/internal/bootstrap"
	"distributed-job-platform/internal/infrastructure/postgres"
	"distributed-job-platform/internal/infrastructure/redisqueue"
	"distributed-job-platform/internal/observability"
	"distributed-job-platform/internal/workers"

	"github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer cancel()

	// Second signal forces immediate exit if graceful shutdown hangs.
	go func() {
		<-ctx.Done()

		second := make(chan os.Signal, 1)
		signal.Notify(second, syscall.SIGINT, syscall.SIGTERM)

		<-second
		os.Exit(1)
	}()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/local.yaml"
	}

	cfg, err := bootstrap.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	logger := bootstrap.NewLogger(cfg.App.Env)

	// ------------------------------------------------------------
	// PostgreSQL
	// ------------------------------------------------------------

	db, err := sql.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		logger.Error("failed to open postgres", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	if err := db.PingContext(ctx); err != nil {
		logger.Error("failed to ping postgres", "error", err)
		os.Exit(1)
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
		logger.Error("failed to ping redis", "error", err)
		os.Exit(1)
	}

	const stream = "jobs"
	const group = "job-workers"

	if err := redisqueue.EnsureConsumerGroup(
		ctx,
		redisClient,
		stream,
		group,
	); err != nil {
		logger.Error(
			"failed to ensure consumer group",
			"error",
			err,
		)
		os.Exit(1)
	}

	consumer, err := redisqueue.NewJobConsumer(
		redisClient,
		stream,
		group,
		5*time.Second,
	)
	if err != nil {
		logger.Error(
			"failed to create queue consumer",
			"error",
			err,
		)
		os.Exit(1)
	}

	producer, err := redisqueue.NewJobQueue(
		redisClient,
		stream,
	)
	if err != nil {
		logger.Error(
			"failed to create queue producer",
			"error",
			err,
		)
		os.Exit(1)
	}

	// ------------------------------------------------------------
	// PostgreSQL repository
	// ------------------------------------------------------------

	repo, err := postgres.NewJobRepository(db)
	if err != nil {
		logger.Error(
			"failed to create job repository",
			"error",
			err,
		)
		os.Exit(1)
	}

	// ------------------------------------------------------------
	// Prometheus metrics
	// ------------------------------------------------------------

	registryMetrics := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registryMetrics)

	// Worker metrics HTTP server.
	metricsAddr := os.Getenv("WORKER_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9091"
	}

	go serveMetrics(
		ctx,
		logger,
		metricsAddr,
		registryMetrics,
	)

	// ------------------------------------------------------------
	// Job service
	// ------------------------------------------------------------

	// Worker uses the queue only for recovery/retry re-enqueue,
	// not for normal job creation.
	jobService, err := appjob.NewService(
		repo,
		producer,
		metrics,
	)
	if err != nil {
		logger.Error(
			"failed to create job service",
			"error",
			err,
		)
		os.Exit(1)
	}

	// ------------------------------------------------------------
	// Worker identity
	// ------------------------------------------------------------

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = fmt.Sprintf("worker-%s", hostname)
	}

	// ------------------------------------------------------------
	// Handler registry
	// ------------------------------------------------------------

	registry := workers.HandlerRegistry{
		"email.send":   workers.NewEmailHandler(logger),
		"pdf.generate": workers.NewPDFHandler(logger),
	}

	// ------------------------------------------------------------
	// Worker configuration
	// ------------------------------------------------------------

	concurrency := envInt(
		"WORKER_CONCURRENCY",
		10,
	)

	batchSize := envInt(
		"WORKER_BATCH_SIZE",
		concurrency,
	)

	// ------------------------------------------------------------
	// Create worker
	// ------------------------------------------------------------

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
		logger.Error(
			"failed to create worker",
			"error",
			err,
		)
		os.Exit(1)
	}

	logger.Info(
		"worker started",
		"worker_id", workerID,
		"concurrency", concurrency,
		"batch_size", batchSize,
		"metrics_addr", metricsAddr,
	)

	// ------------------------------------------------------------
	// Start worker
	// ------------------------------------------------------------

	if err := worker.Run(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		logger.Error(
			"worker stopped with error",
			"error",
			err,
		)
		os.Exit(1)
	}

	logger.Info("worker stopped")
}

// serveMetrics exposes the worker's Prometheus metrics.
//
// Example:
//   http://localhost:9091/metrics
func serveMetrics(
	ctx context.Context,
	logger *slog.Logger,
	addr string,
	registry *prometheus.Registry,
) {
	mux := http.NewServeMux()

	mux.Handle(
		"/metrics",
		promhttp.HandlerFor(
			registry,
			promhttp.HandlerOpts{},
		),
	)

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Gracefully shut down the metrics server
	// when the worker context is cancelled.
	go func() {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error(
				"worker metrics server shutdown failed",
				"error",
				err,
			)
		}
	}()

	logger.Info(
		"worker metrics server started",
		"addr",
		addr,
	)

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		logger.Error(
			"worker metrics server failed",
			"error",
			err,
		)
	}
}

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