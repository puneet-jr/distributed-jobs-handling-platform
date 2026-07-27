package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	appjob "distributed-job-platform/internal/application/job"
	"distributed-job-platform/internal/bootstrap"
	"distributed-job-platform/internal/infrastructure/postgres"
	"distributed-job-platform/internal/infrastructure/redisqueue"
	"distributed-job-platform/internal/workers"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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

	if err := redisqueue.EnsureConsumerGroup(ctx, redisClient, stream, group); err != nil {
		logger.Error("failed to ensure consumer group", "error", err)
		os.Exit(1)
	}

	consumer, err := redisqueue.NewJobConsumer(redisClient, stream, group, 5*time.Second)
	if err != nil {
		logger.Error("failed to create queue consumer", "error", err)
		os.Exit(1)
	}

	producer, err := redisqueue.NewJobQueue(redisClient, stream)
	if err != nil {
		logger.Error("failed to create queue producer", "error", err)
		os.Exit(1)
	}

	repo, err := postgres.NewJobRepository(db)
	if err != nil {
		logger.Error("failed to create job repository", "error", err)
		os.Exit(1)
	}

	// Worker uses the queue only for recovery/retry re-enqueue, not job creation.
	jobService := appjob.NewService(repo, producer)

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, _ := os.Hostname()
		workerID = fmt.Sprintf("worker-%s", hostname)
	}

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
	)
	if err != nil {
		logger.Error("failed to create worker", "error", err)
		os.Exit(1)
	}

	logger.Info("worker started",
		"worker_id", workerID,
		"concurrency", concurrency,
		"batch_size", batchSize,
	)

	if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("worker stopped")
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
