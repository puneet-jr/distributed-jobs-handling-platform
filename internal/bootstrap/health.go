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

func NewHealthHandler(logger *slog.Logger, db *sql.DB, redisClient *redis.Client) http.HandlerFunc {
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

/*  What this piece does in the flow:
load balancer or operator calls /health -> handler probes both acceptance-path
dependencies -> if either durable storage or enqueue path is down, readiness fails. That
makes the endpoint useful for real traffic control, not just a partial liveness check.*/