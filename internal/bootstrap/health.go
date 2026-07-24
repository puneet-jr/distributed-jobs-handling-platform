package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// HealthHandler holds shared dependencies needed by the /health endpoint.
// Dependencies are injected once during startup instead of created per request.
type HealthHandler struct {
	logger *slog.Logger
	db     *sql.DB
}

// Return the handler method because the router expects
// func(http.ResponseWriter, *http.Request).
func NewHealthHandler(logger *slog.Logger, db *sql.DB) http.HandlerFunc {
	h := &HealthHandler{
		logger: logger,
		db:     db,
	}
	return h.ServeHTTP
}

// ServeHTTP handles GET /health requests.
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// Health checks verify the application's current readiness.
	// Startup (bootstrap) only proves the app started successfully.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel() // Always release the context resources.

	// JSON response. 'any' allows values of different types if needed.
	health := map[string]any{
		"status": "healthy",
	}

	// The app may still be running even if the database crashes later.
	// Ping performs a live readiness check.
	if err := h.db.PingContext(ctx); err != nil {
		health["status"] = "unhealthy"
		health["database"] = "down"

		// Log detailed error for debugging.
		h.logger.Error("health check failed", "error", err)

		// Tell clients/load balancers this instance shouldn't receive traffic.
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		health["database"] = "up"
	}

	// Return the health report as JSON.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}
