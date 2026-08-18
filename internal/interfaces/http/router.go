package httpapi

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter(jobHandler *JobHandler, healthHandler http.HandlerFunc) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("POST /jobs", jobHandler.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", jobHandler.GetJob)

	return mux
}
