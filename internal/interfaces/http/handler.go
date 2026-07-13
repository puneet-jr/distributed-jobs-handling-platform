package httpapi

import (
	"encoding/json"
		"net/http"

		appjob "github.com/your-org/distributed-job-platform/internal/application/job"
		"github.com/your-org/distributed-job-platform/internal/shared/response"
)

type JobHandler struct {
	svc *appjob.Service
}

func NewJobHandler(svc *appjob.Service) *JobHandler {
	return &JobHandler{svc:svc}
}

func(h *JobHandler) CreateJob(w http.ResponseWriter,r *http.Request{
	var req CreateJobRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response.Error(w, http.StatusBadRequest, "invalid json")
		return
	}

	out, err := h.svc.Create(e.Context(),appjob.CreateJobRequest{
		Name: req.Name,
		Payload: req.Payload,
	})
	if err != nil {
		response.Error(w,http.StatusBadRequest, err.Error())
		return
	}

	response.JSON(w,http.StatusCreated,out)
}

func(h *JobHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathVlaue("id")

	out,err := h.svc.List(r.Context())

	if err != nil {
		response.Error(w,http.StatusInternalServerError,err.Error())
		return
	}

	response.JSON(w, http.StatusOK, out)
}
