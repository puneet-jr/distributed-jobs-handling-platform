
  metrics:  metrics,

  }, nil
  }

  func (c *QueueDepthCollector) Run(ctx context.Context, logger *slog.Logger) {

  ticker := time.NewTicker(c.interval)

  defer ticker.Stop()


  for {


  if err := c.collect(ctx); err != nil && logger != nil {



  logger.Error("failed to collect queue depth", "error", err)


  }



  select {


  case <-ctx.Done():



  return


  case <-ticker.C:


  }

  }
  }

  func (c *QueueDepthCollector) collect(ctx context.Context) error {

  depth, err := c.client.XLen(ctx, c.stream).Result()

  if err != nil {


  return fmt.Errorf("read redis stream depth: %w", err)

  }


  c.metrics.QueueDepth.Set(float64(depth))

  return nil
  }
  ```

  Then start it in [internal/bootstrap/app.go](/abs/path/C:/Users/punee/Desktop/MY-
  PROJECTS/Projects/Distributed%20Job%20Platform/distributed-job-platform/internal/
  bootstrap/app.go) after metrics are created:

  ```go
  depthCollector, err := observability.NewQueueDepthCollector(

  redisClient,

  "jobs",

  5*time.Second,

  metrics,
  )
  if err != nil {

  _ = redisClient.Close()

  _ = db.Close()

  return nil, fmt.Errorf("failed to create queue depth collector: %w", err)
  }

  go depthCollector.Run(ctx, logger)
  ```

  That gives you a real `queue_depth` series for `k6`.

  **What is not worth doing before `k6`**

  Skip these for now:

  - tracing
  - DLQ
  - cancellation API
  - outbox pattern
  - distributed rate limiting
  - fancy handler plugins

  Those are real features, but they are not the next blocking step.

  **Minimum bar before you run `k6`**

  1. `/metrics` must expose your custom registry metrics.
  2. Worker must never ACK if status persistence failed.
  3. `/health` must fail when Redis or Postgres is unavailable.
  4. `queue_depth` must be real, not just declared.
  5. `go build ./...` must pass with a local `GOCACHE`.

  If you want, next I can review your exact `k6` test shape and tell you what scenarios
  are worth running first for this architecture.-

  so check this code issues and give the final code now- okay code quality the explanation
  and etc path and the why and what that piece of is upto and the flow with other files so
  give the code in here as a response do not edit the code okay and make sure you give the
  code ok teh next step - check plam.ms i think


• I’m checking the current repo state against plan.md and the specific files you called out
  so the response reflects the actual code paths, not assumptions. After that I’ll give you
  the final code in-chat, with file-by-file reasoning and how the pieces connect.

• Ran 7 commands · ctrl + t to view transcript

───────────────────────────────────────────────────────────────────────────────────────────

• The repo confirms the main gaps are exactly where you pointed: metrics wiring, health
  dependency coverage, worker ACK semantics, and real queue-depth collection. I’m doing one
  quick pass over worker/bootstrap support files so the code I give you matches the
  existing types and startup flow.

• Ran 5 commands · ctrl + t to view transcript

───────────────────────────────────────────────────────────────────────────────────────────

• plan.md is stale. The repo already has the async path, worker, Postgres, Redis Streams,
  and Prometheus metric definitions. The pre-k6 gaps are operational correctness and signal
  quality, not platform scaffolding.

  The four code changes below are the right next step. I am not editing the repo here; this
  is the exact code to apply and why each piece exists.

  1. Fix /metrics so it serves the custom registry

  Why this matters: in /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-
  job-platform/internal/bootstrap/app.go you create a dedicated Prometheus registry, but /
  abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-platform/internal/
  interfaces/http/router.go currently serves promhttp.Handler(), which uses the global
  default registry. That means your counters can increment and still never appear on /
  metrics.

  Replace /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-platform/
  internal/interfaces/http/router.go with:

  package httpapi

  import "net/http"

  func NewRouter(
        jobHandler *JobHandler,
        healthHandler http.HandlerFunc,
        metricsHandler http.Handler,
  ) http.Handler {
        mux := http.NewServeMux()

        mux.HandleFunc("GET /health", healthHandler)
        mux.Handle("GET /metrics", metricsHandler)

        mux.HandleFunc("POST /jobs", jobHandler.CreateJob)
        mux.HandleFunc("GET /jobs/{id}", jobHandler.GetJob)

        return mux
  }

  Then update the relevant part of /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/
  distributed-job-platform/internal/bootstrap/app.go:

  package bootstrap

  import (
        "context"
        "database/sql"
        "fmt"
        "log/slog"
        "net/http"
        "time"

        "github.com/prometheus/client_golang/prometheus"
        "github.com/prometheus/client_golang/prometheus/promhttp"
        "github.com/redis/go-redis/v9"

        appjob "distributed-job-platform/internal/application/job"
        "distributed-job-platform/internal/infrastructure/postgres"
        redisqueue "distributed-job-platform/internal/infrastructure/redisqueue"
        httpapi "distributed-job-platform/internal/interfaces/http"
        "distributed-job-platform/internal/observability"
  )

  And change router wiring to:

        registry := prometheus.NewRegistry()
        metrics := observability.NewMetrics(registry)

        jobService, err := appjob.NewService(repo, jobQueue, metrics)
        if err != nil {
                _ = redisClient.Close()
                _ = db.Close()
                return nil, fmt.Errorf("failed to create job service: %w", err)
        }

        jobHandler := httpapi.NewJobHandler(jobService)
        metricsHandler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

        router := httpapi.NewRouter(
                jobHandler,
                NewHealthHandler(logger, db, redisClient),
                metricsHandler,
        )

  What this piece does in the flow:
  app.go owns the registry -> observability.NewMetrics registers the counters/gauges on
  that registry -> router exposes that exact registry on /metrics -> Prometheus now sees
  the metrics your app is actually recording.

  2. Do not ACK Redis if retry persistence failed

  Why this matters: in /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-
  job-platform/internal/workers/worker.go, the handler failure branch currently calls
  MarkRetrying, ignores its error, and ACKs the Redis message anyway. If Postgres is
  unavailable in that moment, the retry state is never saved and the queue message is gone.
  That is silent job loss.

  Replace the handler-failure branch inside processMessage with:

        if err := handler.Handle(ctx, *job); err != nil {
                // Persist retry state before ACKing.
                // If this fails, leave the message pending so Redis can redeliver it.
                if retryErr := w.service.MarkRetrying(ctx, msg.JobID, w.id, err.Error()); retryErr != nil
                {
                        logger.Error(
                                "failed to persist retry state; leaving message unacked for redelivery",
                                "handler_error", err,
                                "retry_error", retryErr,
                        )
                        return
                }

                if ackErr := w.queue.Ack(ctx, msg.ID); ackErr != nil {
                        logger.Error("failed to ack message after retry scheduling", "error", ackErr)
                        return
                }

                logger.Error(
                        "job failed and retry state was persisted",
                        "error", err,
                        "duration_ms", time.Since(start).Milliseconds(),
                )

                if w.metrics != nil {
                        w.metrics.HandlerErrors.WithLabelValues(job.Type, w.id).Inc()
                        w.metrics.JobsRetried.WithLabelValues(job.Type, w.id).Inc()
                        w.metrics.JobProcessingTime.WithLabelValues(job.Type,
                        w.id).Observe(time.Since(start).Seconds())
                }

                return
        }

  What this piece does in the flow:
  worker reads Redis message -> loads job -> handler fails -> service persists retry/
  failure state in Postgres -> only after durable state change does the worker ACK Redis.
  That preserves at-least-once delivery instead of losing work.

  3. Make /health reflect Redis and Postgres

  Why this matters: your API path is persist-then-enqueue. If Redis is down, POST /jobs
  cannot truthfully accept work. The current /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/
  Projects/distributed-job-platform/internal/bootstrap/health.go only checks Postgres, so
  it can report healthy while the app cannot enqueue jobs.

  Replace /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-platform/
  internal/bootstrap/health.go with:

  package bootstrap

  import (
        "context"
        "database/sql"
        "encoding/json"
        "log/slog"
        "net/http"
        "time"

        "github.com/redis/go-redis/v9"
  )

  type HealthHandler struct {
        logger *slog.Logger
        db     *sql.DB
        redis  *redis.Client
  }

  func NewHealthHandler(logger *slog.Logger, db *sql.DB, redisClient *redis.Client)
  http.HandlerFunc {
        h := &HealthHandler{
                logger: logger,
                db:     db,
                redis:  redisClient,
        }
        return h.ServeHTTP
  }

  func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
        ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
        defer cancel()

        health := map[string]any{
                "status":   "healthy",
                "database": "up",
                "redis":    "up",
        }

        statusCode := http.StatusOK

        if err := h.db.PingContext(ctx); err != nil {
                health["status"] = "unhealthy"
                health["database"] = "down"
                statusCode = http.StatusServiceUnavailable
                h.logger.Error("database health check failed", "error", err)
        }

        if err := h.redis.Ping(ctx).Err(); err != nil {
                health["status"] = "unhealthy"
                health["redis"] = "down"
                statusCode = http.StatusServiceUnavailable
                h.logger.Error("redis health check failed", "error", err)
        }

        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(statusCode)
        _ = json.NewEncoder(w).Encode(health)
  }

  What this piece does in the flow:
  load balancer or operator calls /health -> handler probes both acceptance-path
  dependencies -> if either durable storage or enqueue path is down, readiness fails. That
  makes the endpoint useful for real traffic control, not just a partial liveness check.

  4. Make queue_depth a real metric

  Why this matters: /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-
  platform/internal/observability/metrics.go defines QueueDepth, but nothing updates it.
  Before k6, you need to know whether the system is actually keeping up or just buffering
  backlog in Redis.

  Add /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-platform/
  internal/observability/queue_depth.go:

  package observability

  import (
        "context"
        "fmt"
        "log/slog"
        "time"

        "github.com/redis/go-redis/v9"
  )

  type QueueDepthCollector struct {
        client   *redis.Client
        stream   string
        interval time.Duration
        metrics  *Metrics
  }

  func NewQueueDepthCollector(
        client *redis.Client,
        stream string,
        interval time.Duration,
        metrics *Metrics,
  ) (*QueueDepthCollector, error) {
        if client == nil {
                return nil, fmt.Errorf("redis client is required")
        }
        if stream == "" {
                return nil, fmt.Errorf("stream name is required")
        }
        if interval <= 0 {
                interval = 5 * time.Second
        }
        if metrics == nil {
                return nil, fmt.Errorf("metrics are required")
        }

        return &QueueDepthCollector{
                client:   client,
                stream:   stream,
                interval: interval,
                metrics:  metrics,
        }, nil
  }

  func (c *QueueDepthCollector) Run(ctx context.Context, logger *slog.Logger) {
        ticker := time.NewTicker(c.interval)
        defer ticker.Stop()

        for {
                if err := c.collect(ctx); err != nil && logger != nil {
                        logger.Error("failed to collect queue depth", "error", err)
                }

                select {
                case <-ctx.Done():
                        return
                case <-ticker.C:
                }
        }
  }

  func (c *QueueDepthCollector) collect(ctx context.Context) error {
        depth, err := c.client.XLen(ctx, c.stream).Result()
        if err != nil {
                return fmt.Errorf("read redis stream depth: %w", err)
        }

        c.metrics.QueueDepth.Set(float64(depth))
        return nil
  }

  Then wire it in /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-
  platform/internal/bootstrap/app.go after metrics creation:

        registry := prometheus.NewRegistry()
        metrics := observability.NewMetrics(registry)

        depthCollector, err := observability.NewQueueDepthCollector(
                redisClient,
                "jobs",
                5*time.Second,
                metrics,
        )
        if err != nil {
                _ = redisClient.Close()
                _ = db.Close()
                return nil, fmt.Errorf("failed to create queue depth collector: %w", err)
        }

        go depthCollector.Run(ctx, logger)

  What this piece does in the flow:
  Redis stream is the shared queue state -> collector polls XLEN -> updates gauge on the
  same custom registry -> /metrics exposes backlog alongside throughput and error metrics.
  That makes k6 results interpretable.

  One more repo note

  /abs/path/C:/Users/punee/Desktop/MY-PROJECTS/Projects/distributed-job-platform/internal/
  bootstrap/metrics.go appears to be old unused scaffolding with atomic counters. Given the
  current Prometheus-based path in internal/observability, it is probably stale and should
  not be expanded further. Keep one metrics path, not two.

  How these files connect

  internal/bootstrap/app.go is the composition root. It builds DB, Redis, queue, repo,
  service, metrics, health handler, and router.

  internal/interfaces/http/router.go is just the edge wiring. It should not create infra
  handlers on its own; it should receive them from bootstrap.

  internal/bootstrap/health.go reports readiness for the API acceptance path.

  internal/workers/worker.go is where queue-delivery guarantees meet durable state
  transitions. This is the most important correctness point before load testing.

  internal/observability/metrics.go defines the metric instruments.

  internal/observability/queue_depth.go turns the declared queue gauge into real runtime
  data.

  Minimum bar before k6

  1. go build ./... passes.
  2. /metrics shows jobs_created_total, worker_poll_errors_total, queue_depth, and
     processing histograms.

  3. Worker does not ACK when retry persistence fails.
  4. /health returns 503 if Redis or Postgres is unavailable.
  5. Under a small manual test, queue depth rises when workers are stopped and drains when
     they resume.

  If you want, the next useful step is for me to give you the exact k6 scenario file shape
  for this architecture: smoke, sustained throughput, backlog growth, and worker-failure
  recovery.
