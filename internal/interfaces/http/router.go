package httpapi

import "net/http"

func NewRouter(jobHandler *JobHandler, healthHandler http.HandlerFunc, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", healthHandler)
	mux.Handle("GET /metrics", metricsHandler)

	mux.HandleFunc("POST /jobs", jobHandler.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", jobHandler.GetJob)

	return mux
}
