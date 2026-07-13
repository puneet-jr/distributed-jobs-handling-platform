package httpi

import (
	"net/http"
)

func NewRouter(job *JobHandler, health http.HandlerFuc) http.Handler{
	mux:= http.NewServerMux()

	mux.HandleFunc("GET /health",health)
	mux.HanldeFunc("POST /jobs",job.CreateJob)
	mux.HandleFunc("GET /jobs", jobs.GetJob)
	return mux
}


