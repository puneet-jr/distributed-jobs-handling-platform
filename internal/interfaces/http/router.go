package httpapi

import (
	"net/http"
)

func NewRouter(jobHandler *JobHandler, healthHandler http.HandlerFunc) http.Handler {
	mux := http.NewServeMux()
	// POST /jobs: create new job (idempotent with header)
	// GET /jobs/{id}: fetch job status
	// GET /health: load balancer health check
	
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /jobs", jobHandler.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", jobHandler.GetJob)
	
	return mux
}
