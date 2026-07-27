package httpapi

import (
	"encoding/json"
	"net/http"

	appjob "distributed-job-platform/internal/application/job"
	"distributed-job-platform/internal/shared/response"
)

type JobHandler struct {
	svc *appjob.Service
}

func NewJobHandler(svc *appjob.Service) *JobHandler {
	return &JobHandler{svc: svc}
}

func (h *JobHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	// Why check Content-Type?
	// Prevents accidental form submissions or wrong payload types.
	// Enforces API contract: "I only accept JSON".
	if r.Header.Get("Content-Type") != "application/json" {
		response.Error(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	var req CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Why get idempotency key from header?
	// Client controls idempotency, not server.
	// Header is standard location: "Idempotency-Key: abc123"
	idempotencyKey := r.Header.Get("Idempotency-Key")

	// Why call service?
	// Handler is thin: extract from HTTP, delegate to service.
	// Service contains business logic, validation, transactions.
	out, err := h.svc.Create(r.Context(), appjob.CreateJobRequest{
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		response.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	// Why 202 Accepted?
	// Job is queued, not completed.
	// 201 Created implies resource is ready. 202 says "we accepted it, processing later".
	response.JSON(w, http.StatusAccepted, out)
}

func (h *JobHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	// Why use PathValue?
	// router.go has: GET /jobs/{id}
	// r.PathValue("id") extracts the id.
	id := r.PathValue("id")
	if id == "" {
		response.Error(w, http.StatusBadRequest, "missing job id")
		return
	}

	out, err := h.svc.GetByID(r.Context(), id)
	if err != nil {
		response.Error(w, http.StatusNotFound, "job not found")
		return
	}

	response.JSON(w, http.StatusOK, out)
}
